package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcutil"
	//"github.com/davecgh/go-spew/spew"
	"github.com/go-redis/redis"
)

// importResults houses the stats and result as an import operation.
type importResults struct {
	blocksProcessed int64
	err             error
}

// blockImporter houses information about an ongoing import from a block data
// file to the block database.
type blockImporter struct {
	r                 io.ReadSeeker
	c                 *redis.Client
	rpc               *rpcclient.Client
	f                 *os.File
	f2                *os.File
	processQueue      chan []byte
	doneChan          chan bool
	errChan           chan error
	quit              chan struct{}
	wg                sync.WaitGroup
	blocksProcessed   int64
	receivedLogBlocks int64
	receivedLogTx     int64
	lastHeight        int64
	lastBlockTime     time.Time
	lastLogTime       time.Time
}

// readBlock reads the next block from the input file.
func (bi *blockImporter) readBlock() ([]byte, error) {
	// The block file format is:
	//  <network> <block length> <serialized block>
	var net uint32
	err := binary.Read(bi.r, binary.LittleEndian, &net)
	if err != nil {
		if err != io.EOF {
			return nil, err
		}

		// No block and no error means there are no more blocks to read.
		return nil, nil
	}
	if net != uint32(activeNetParams.Net) {
		return nil, fmt.Errorf("network mismatch -- got %x, want %x",
			net, uint32(activeNetParams.Net))
	}

	// Read the block length and ensure it is sane.
	var blockLen uint32
	if err := binary.Read(bi.r, binary.LittleEndian, &blockLen); err != nil {
		return nil, err
	}

	serializedBlock := make([]byte, blockLen)
	if _, err := io.ReadFull(bi.r, serializedBlock); err != nil {
		return nil, err
	}

	return serializedBlock, nil
}

// processBlock potentially imports the block into the database.  It first
// deserializes the raw block while checking for errors.  Already known blocks
// are skipped and orphan blocks are considered errors.  Finally, it runs the
// block through the chain rules to ensure it follows all rules and matches
// up to the known checkpoint.  Returns whether the block was imported along
// with any potential errors.
func (bi *blockImporter) processBlock(serializedBlock []byte) (bool, error) {
	var in, out map[string]bool

	// Deserialize the block which includes checks for malformed blocks.
	block, err := btcutil.NewBlockFromBytes(serializedBlock)
	if err != nil {
		return false, err
	}

	for _, tx := range block.MsgBlock().Transactions {
		in = make(map[string]bool)
		out = make(map[string]bool)
		for _, input := range tx.TxIn {
			parts := strings.Split(input.PreviousOutPoint.String(), `:`)
			if parts[0] != `0000000000000000000000000000000000000000000000000000000000000000` {
				hash, err := chainhash.NewHashFromStr(parts[0])
				if err != nil {
					return false, err
				}
				prevtx, err := bi.rpc.GetRawTransaction(hash)
				if err != nil {
					return false, err
				}

				if prevtx != nil {
					idx, _ := strconv.Atoi(parts[1])
					prevtxout := prevtx.MsgTx().TxOut[idx]
					_, addrs, _, err := txscript.ExtractPkScriptAddrs(prevtxout.PkScript, &chaincfg.MainNetParams)
					if err != nil {
						return false, err
					}
					for _, addr := range addrs {
						in[addr.EncodeAddress()] = true
					}
				}
			} else {
				in[`coinbase`] = true
			}
		}

		for _, output := range tx.TxOut {
			_, addrs, _, err := txscript.ExtractPkScriptAddrs(output.PkScript, &chaincfg.MainNetParams)
			if err != nil {
				return false, err
			}
			for _, addr := range addrs {
				out[addr.EncodeAddress()] = true
			}

		}

		if err := bi.Save(in, out); err != nil {
			return false, err
		}
	}

	return true, nil
}

func (bi *blockImporter) Save(i, o map[string]bool) error {
	var keys, in, out []string

	for v, _ := range i {
		in = append(in, v)
	}
	for v, _ := range o {
		out = append(out, v)
	}

	j, err := json.Marshal(Request{
		In:  in,
		Out: out,
	})
	if err != nil {
		return err
	}

	keys = append(keys, fmt.Sprintf("%s", string(j)))
	r, err := bi.c.EvalSha(script, keys).Result()
	if err != nil {
		return err
	}

	ret := strings.Replace(r.(string), "{}", "[]", -1)

	resp := Response{}
	err = json.Unmarshal([]byte(ret), &resp)
	if err != nil {
		return err
	}

	for _, v := range resp.Pairs {
		bi.f.WriteString(v + "\n")
	}

	for _, v := range resp.Edges {
		bi.f2.WriteString(v.ID + "," + v.Address + "\n")
	}

	return nil
}

// readHandler is the main handler for reading blocks from the import file.
// This allows block processing to take place in parallel with block reads.
// It must be run as a goroutine.
func (bi *blockImporter) readHandler() {
out:
	for {
		// Read the next block from the file and if anything goes wrong
		// notify the status handler with the error and bail.
		serializedBlock, err := bi.readBlock()
		if err != nil {
			bi.errChan <- fmt.Errorf("Error reading from input "+
				"file: %v", err.Error())
			break out
		}

		// A nil block with no error means we're done.
		if serializedBlock == nil {
			break out
		}

		// Send the block or quit if we've been signalled to exit by
		// the status handler due to an error elsewhere.
		select {
		case bi.processQueue <- serializedBlock:
		case <-bi.quit:
			break out
		}
	}

	// Close the processing channel to signal no more blocks are coming.
	close(bi.processQueue)
	bi.wg.Done()
}

// processHandler is the main handler for processing blocks.  This allows block
// processing to take place in parallel with block reads from the import file.
// It must be run as a goroutine.
func (bi *blockImporter) processHandler() {
out:
	for {
		select {
		case serializedBlock, ok := <-bi.processQueue:
			// We're done when the channel is closed.
			if !ok {
				break out
			}

			bi.blocksProcessed++
			bi.lastHeight++
			_, err := bi.processBlock(serializedBlock)
			if err != nil {
				bi.errChan <- err
				break out
			}

		case <-bi.quit:
			break out
		}
	}
	bi.wg.Done()
}

// statusHandler waits for updates from the import operation and notifies
// the passed doneChan with the results of the import.  It also causes all
// goroutines to exit if an error is reported from any of them.
func (bi *blockImporter) statusHandler(resultsChan chan *importResults) {
	select {
	// An error from either of the goroutines means we're done so signal
	// caller with the error and signal all goroutines to quit.
	case err := <-bi.errChan:
		resultsChan <- &importResults{
			blocksProcessed: bi.blocksProcessed,
			err:             err,
		}
		close(bi.quit)

	// The import finished normally.
	case <-bi.doneChan:
		resultsChan <- &importResults{
			blocksProcessed: bi.blocksProcessed,
			err:             nil,
		}
	}
}

// Import is the core function which handles importing the blocks from the file
// associated with the block importer to the database.  It returns a channel
// on which the results will be returned when the operation has completed.
func (bi *blockImporter) Import() chan *importResults {
	// Start up the read and process handling goroutines.  This setup allows
	// blocks to be read from disk in parallel while being processed.
	bi.wg.Add(2)
	go bi.readHandler()
	go bi.processHandler()

	// Wait for the import to finish in a separate goroutine and signal
	// the status handler when done.
	go func() {
		bi.wg.Wait()
		bi.doneChan <- true
	}()

	// Start the status handler and return the result channel that it will
	// send the results on when the import is done.
	resultChan := make(chan *importResults)
	go bi.statusHandler(resultChan)
	return resultChan
}

// newBlockImporter returns a new importer for the provided file reader seeker
// and database.
func newBlockImporter(r io.ReadSeeker, rpc *rpcclient.Client, c *redis.Client, f *os.File, f2 *os.File) (*blockImporter, error) {
	return &blockImporter{
		r:            r,
		c:            c,
		rpc:          rpc,
		f:            f,
		f2:           f2,
		processQueue: make(chan []byte, 2),
		doneChan:     make(chan bool),
		errChan:      make(chan error),
		quit:         make(chan struct{}),
	}, nil
}
