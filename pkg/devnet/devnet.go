package devnet

import (
	"context"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

func Run(ctx context.Context, done chan struct{}) {
	var wg sync.WaitGroup

	// The parameter files can be as large as 1GiB.
	// If this is the first time lotus runs,
	// and the machine doesn't have particularly fast internet,
	// we don't want devnet to seemingly stall for many minutes.
	// Instead, show the download progress explicitly.
	// fetch-params will exit in about a second if all files are up to date.
	// The command is also pretty verbose, so reduce its verbosity.
	{
		// Ten minutes should be enough for practically any machine.
		ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)

		log.Println("Running 'lotus fetch-params 2048'...")
		cmd := exec.CommandContext(ctx, "lotus", "fetch-params", "2048")
		cmd.Env = append(os.Environ(), "GOLOG_LOG_LEVEL=error")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}
		cancel()
	}

	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}

	wg.Add(4)
	go func() {
		runLotusDaemon(ctx, home)
		log.Println("shut down lotus daemon")
		wg.Done()
	}()

	go func() {
		runLotusMiner(ctx, home)
		log.Println("shut down lotus miner")
		wg.Done()
	}()

	go func() {
		publishDealsPeriodicallyCmd(ctx)
		wg.Done()
	}()

	go func() {
		setDefaultWalletCmd(ctx)
		wg.Done()
	}()

	wg.Wait()

	done <- struct{}{}
}

func runCmdsWithLog(ctx context.Context, name string, commands [][]string) {
	logFile, err := os.Create(name + ".log")
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	for _, cmdArgs := range commands {
		log.Printf("command for %s: %s", name, strings.Join(cmdArgs, " "))
		cmd := exec.CommandContext(ctx, cmdArgs[0], cmdArgs[1:]...)
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		// If ctx.Err()!=nil, we cancelled the command via SIGINT.
		if err := cmd.Run(); err != nil && ctx.Err() == nil {
			log.Printf("%s; check %s for details", err, logFile.Name())
			break
		}
	}
}

func runLotusDaemon(ctx context.Context, home string) {
	cmds := [][]string{
		{"lotus-seed", "genesis", "new", "localnet.json"},
		{"lotus-seed", "pre-seal", "--sector-size=2048", "--num-sectors=10"},
		{"lotus-seed", "genesis", "add-miner", "localnet.json",
			filepath.Join(home, ".genesis-sectors", "pre-seal-t01000.json")},
		{"lotus", "daemon", "--lotus-make-genesis=dev.gen",
			"--genesis-template=localnet.json", "--bootstrap=false"},
	}

	runCmdsWithLog(ctx, "lotus-daemon", cmds)
}

func runLotusMiner(ctx context.Context, home string) {
	cmds := [][]string{
		{"lotus", "wait-api"}, // wait for lotus node to run

		{"lotus", "wallet", "import",
			filepath.Join(home, ".genesis-sectors", "pre-seal-t01000.key")},
		{"lotus-miner", "init", "--genesis-miner", "--actor=t01000", "--sector-size=2048",
			"--pre-sealed-sectors=" + filepath.Join(home, ".genesis-sectors"),
			"--pre-sealed-metadata=" + filepath.Join(home, ".genesis-sectors", "pre-seal-t01000.json"),
			"--nosync"},

		// Starting in network version 13,
		// pre-commits are batched by default,
		// and commits are aggregated by default.
		// This means deals could sit at StorageDealAwaitingPreCommit or
		// StorageDealSealing for a while, going past our 10m test timeout.
		{"sed", "-ri", "-e", "s/#BatchPreCommits\\ =\\ true/BatchPreCommits=false/",
			filepath.Join(home, ".lotusminer", "config.toml")},

		{"sed", "-ri", "-e", "s/#AggregateCommits\\ =\\ true/AggregateCommits=false/",
			filepath.Join(home, ".lotusminer", "config.toml")},

		{"lotus-miner", "run", "--nosync"},
	}

	runCmdsWithLog(ctx, "lotus-miner", cmds)
}

func publishDealsPeriodicallyCmd(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}

		cmd := exec.CommandContext(ctx, "lotus-miner",
			"storage-deals", "pending-publish", "--publish-now")
		_ = cmd.Run() // we ignore errors
	}
}

func setDefaultWalletCmd(ctx context.Context) {
	// TODO: do this without a shell
	setDefaultWalletCmd := "lotus wallet list | grep t3 | awk '{print $1}' | xargs lotus wallet set-default"

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}

		cmd := exec.CommandContext(ctx, "sh", "-c", setDefaultWalletCmd)
		_, err := cmd.CombinedOutput()
		if err != nil {
			continue
		}
		// TODO: stop once we've set the default wallet once.
	}
}
