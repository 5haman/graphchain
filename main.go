package main

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"

	log "github.com/albrow/prtty"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/go-redis/redis"
)

var (
	cfg    *config
	c      *redis.Client
	rpc    *rpcclient.Client
	script string
)

// realMain is the real main function for the utility.  It is necessary to work
// around the fact that deferred functions do not run when os.Exit() is called.
func realMain() error {
	// Load configuration and parse command line.
	tcfg, _, err := loadConfig()
	if err != nil {
		return err
	}
	cfg = tcfg

	rpc, err := rpcclient.New(&rpcclient.ConnConfig{
		Host:         cfg.RpcHost,
		User:         cfg.RpcUser,
		Pass:         cfg.RpcPass,
		HTTPPostMode: true, // Bitcoin core only supports HTTP POST mode
		DisableTLS:   true, // Bitcoin core does not provide TLS by default
	}, nil)
	if err != nil {
		log.Error.Fatal("Failed to connect to bitcoind: %v", err)
	}
	defer rpc.Shutdown()

	c := redis.NewClient(&redis.Options{
		Addr: cfg.RedisHost,
	})
	defer c.Close()

	script = ScriptLoad(c)

	f, err := os.Create(cfg.OutputDir + "/edges.csv")
	if err != nil {
		log.Error.Fatal(err)
	}
	defer f.Close()

	f2, err := os.Create(cfg.OutputDir + "/nodes.csv")
	if err != nil {
		log.Error.Fatal(err)
	}
	defer f2.Close()

	log.Info.Println("Starting import")
	if err := filepath.Walk(cfg.BlocksDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Error.Printf("Failed to open dir %v: %v", path, err)
			return err
		}
		if strings.HasPrefix(info.Name(), "blk") && strings.HasSuffix(info.Name(), ".dat") {
			log.Info.Printf("Processing: %v", path)

			fi, err := os.Open(path)
			if err != nil {
				log.Error.Printf("Failed to open file %v: %v", path, err)
				return err
			}
			defer fi.Close()

			// Create a block importer for the database and input file and start it.
			// The done channel returned from start will contain an error if
			// anything went wrong.
			importer, err := newBlockImporter(fi, rpc, c, f, f2)
			if err != nil {
				log.Error.Printf("Failed create block importer: %v", err)
				return err
			}

			// Perform the import asynchronously.  This allows blocks to be
			// processed and read in parallel.  The results channel returned from
			// Import contains the statistics about the import including an error
			// if something went wrong.
			resultsChan := importer.Import()
			results := <-resultsChan
			if results.err != nil {
				log.Error.Printf("%v", results.err)
				return results.err
			}

			log.Info.Printf("Processed %d blocks", results.blocksProcessed)
		}
		return nil
	}); err != nil {
		log.Error.Printf("Failed to process blockchain: %v", err)
	}

	return nil
}

func main() {
	// Use all processor cores and up some limits.
	runtime.GOMAXPROCS(runtime.NumCPU())

	// Work around defer not working after os.Exit()
	if err := realMain(); err != nil {
		os.Exit(1)
	}
}
