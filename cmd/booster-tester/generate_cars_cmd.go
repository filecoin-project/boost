package main

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/urfave/cli/v2"
)

var generateCarsCmd = &cli.Command{
	Name:        "generate-cars",
	Usage:       "",
	Description: "Generates CAR files",
	Flags:       []cli.Flag{},
	ArgsUsage:   "<file-paths>",
	Action: func(cctx *cli.Context) error {
		log.Debugln("args", cctx.Args().Slice())

		if cctx.Args().Len() < 1 {
			return fmt.Errorf("must provide at least one file path")
		}

		files := cctx.Args().Slice()

		var wg sync.WaitGroup

		for _, file := range files {
			wg.Add(1)

			file := file

			go func() {
				defer wg.Done()
				runBoostxGenerateCar(file)
			}()
		}

		wg.Wait()

		return nil
	},
}

func runBoostxGenerateCar(file string) {
	ext := filepath.Ext(file)
	car := file[0:len(file)-len(ext)] + ".car"

	cmd := exec.Command("boostx", "generate-car", file, car)

	cmd.Run()
}
