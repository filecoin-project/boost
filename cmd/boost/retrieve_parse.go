package main

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/filecoin-project/go-address"
	"github.com/google/uuid"
	"github.com/urfave/cli/v2"
)

// Read a single miner from the CLI, returning address.Undef if none is
// provided.
func parseMiner(cctx *cli.Context) (address.Address, error) {
	minerStringRaw := cctx.String(flagMiner.Name)

	miner, err := address.NewFromString(minerStringRaw)
	if err != nil {
		return address.Undef, fmt.Errorf("failed to parse miner: %s: %w", minerStringRaw, err)
	}

	return miner, nil
}

// Read a comma-separated or multi flag list of miners from the CLI.
func parseMiners(cctx *cli.Context) ([]address.Address, error) {
	// Each minerStringsRaw element may contain multiple comma-separated values
	minerStringsRaw := cctx.StringSlice(flagMiners.Name)

	// Split any comma-separated minerStringsRaw elements
	var minerStrings []string
	for _, raw := range minerStringsRaw {
		minerStrings = append(minerStrings, strings.Split(raw, ",")...)
	}

	var miners []address.Address
	for _, ms := range minerStrings {

		miner, err := address.NewFromString(ms)
		if err != nil {
			return nil, fmt.Errorf("failed to parse miner %s: %w", ms, err)
		}

		miners = append(miners, miner)
	}

	return miners, nil
}

// Get whether to use a verified deal or not.
func parseVerified(cctx *cli.Context) bool {
	return cctx.Bool(flagVerified.Name)
}

// Get whether to ask SP to remove unsealed copy.
func parseRemoveUnsealed(cctx *cli.Context) bool {
	return cctx.Bool(removeUnsealed.Name)
}

// Get the destination file to write the output to, erroring if not a valid
// path. This early error check is important because you don't want to do a
// bunch of work, only to end up crashing when you try to write the file.
func parseOutput(cctx *cli.Context) (string, error) {
	path := cctx.String(flagOutput.Name)

	if path != "" && !fs.ValidPath(path) {
		return "", fmt.Errorf("invalid output location '%s'", path)
	}

	return path, nil
}

func parseDealUUID(cctx *cli.Context) (uuid.UUID, error) {
	dealUUIDStr := cctx.String(flagDealUUID.Name)

	if dealUUIDStr == "" {
		return uuid.Nil, nil
	}

	dealUUID, err := uuid.Parse(dealUUIDStr)
	if err != nil {
		return uuid.Nil, fmt.Errorf("failed to parse deal UUID '%s'", dealUUIDStr)
	}

	return dealUUID, nil
}
