package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/chzyer/readline"
	"golang.org/x/xerrors"
)

func confirm(ctx context.Context) (bool, error) {
	cs := readline.NewCancelableStdin(os.Stdin)
	go func() {
		<-ctx.Done()
		cs.Close() // nolint:errcheck
	}()
	rl := bufio.NewReader(cs)
	for {
		fmt.Printf("Proceed? Yes [y] / No [n]:\n")

		line, _, err := rl.ReadLine()
		if err != nil {
			if xerrors.Is(err, io.EOF) {
				return false, fmt.Errorf("request canceled: %w", err)
			}

			return false, fmt.Errorf("reading input: %w", err)
		}

		switch string(line) {
		case "yes", "y":
			return true, nil
		case "n":
			return false, nil
		default:
			return false, nil
		}
	}
}
