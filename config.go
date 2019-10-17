package main

import (
	"os"

	"github.com/btcsuite/btcd/chaincfg"
	flags "github.com/jessevdk/go-flags"
)

var (
	activeNetParams = &chaincfg.MainNetParams
)

// config defines the configuration options for findcheckpoint.
type config struct {
	BlocksDir string `short:"b" long:"blocksdir" description:"Directory with bitcoin blocks files"`
	RpcHost string `short:"h" long:"host" description:"Bitcoind connection"`
	RpcUser string `short:"u" long:"user" description:"Bitcoind user"`
	RpcPass string `short:"p" long:"pass" description:"Bitcoind password"`
	RedisHost string `short:"r" long:"redis" description:"Redis connection"`
	OutputDir string `short:"o" long:"outdir" description:"Result files output directory"`
}

// loadConfig initializes and parses the config using command line options.
func loadConfig() (*config, []string, error) {
	// Default config.
	cfg := config{
		BlocksDir: `~/.bitcoin/blocks`,
		RpcHost: `127.0.0.1:8332`,
		RpcUser: `bitcoin`,
		RpcPass: ``,
		RedisHost: `127.0.0.1:6379`,
		OutputDir: `./output`,
	}

	// Parse command line options.
	parser := flags.NewParser(&cfg, flags.Default)
	remainingArgs, err := parser.Parse()
	if err != nil {
		if e, ok := err.(*flags.Error); !ok || e.Type != flags.ErrHelp {
			parser.WriteHelp(os.Stderr)
		}
		return nil, nil, err
	}

	return &cfg, remainingArgs, nil
}
