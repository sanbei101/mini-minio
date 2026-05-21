package erasure

import (
	"context"
	"io"
	"sync"

	"github.com/klauspost/reedsolomon"
)

const BlockSize = 10 << 20 // 10 MiB

type Erasure struct {
	encoder                  func() reedsolomon.Encoder
	dataBlocks, parityBlocks int
	blockSize                int64
}

func New(dataBlocks, parityBlocks int) (Erasure, error) {
	e := Erasure{
		dataBlocks:   dataBlocks,
		parityBlocks: parityBlocks,
		blockSize:    BlockSize,
	}
	var enc reedsolomon.Encoder
	var once sync.Once
	e.encoder = func() reedsolomon.Encoder {
		once.Do(func() {
			enc, _ = reedsolomon.New(dataBlocks, parityBlocks,
				reedsolomon.WithAutoGoroutines(int(e.ShardSize())))
		})
		return enc
	}
	return e, nil
}

func (e *Erasure) DataBlocks() int   { return e.dataBlocks }
func (e *Erasure) ParityBlocks() int { return e.parityBlocks }
func (e *Erasure) BlockSize() int64  { return e.blockSize }

func (e *Erasure) ShardSize() int64 {
	return ceilFrac(e.blockSize, int64(e.dataBlocks))
}

func (e *Erasure) ShardFileSize(totalLength int64) int64 {
	if totalLength == 0 {
		return 0
	}
	numShards := totalLength / e.blockSize
	lastBlockSize := totalLength % e.blockSize
	lastShardSize := ceilFrac(lastBlockSize, int64(e.dataBlocks))
	return numShards*e.ShardSize() + lastShardSize
}

func (e *Erasure) EncodeData(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, e.dataBlocks+e.parityBlocks), nil
	}
	shards, err := e.encoder().Split(data)
	if err != nil {
		return nil, err
	}
	if err = e.encoder().Encode(shards); err != nil {
		return nil, err
	}
	return shards, nil
}

func (e *Erasure) DecodeDataBlocks(data [][]byte) error {
	missing := 0
	for _, b := range data {
		if len(b) == 0 {
			missing++
		}
	}
	if missing == 0 || missing == len(data) {
		return nil
	}
	return e.encoder().ReconstructData(data)
}

// Encode reads from src, erasure-encodes each block, and writes shards to writers.
func (e *Erasure) Encode(ctx context.Context, src io.Reader, writers []io.Writer, quorum int) (int64, error) {
	buf := make([]byte, e.blockSize)
	var total int64
	errs := make([]error, len(writers))

	for {
		n, err := io.ReadFull(src, buf)
		eof := err == io.EOF || err == io.ErrUnexpectedEOF
		if err != nil && !eof {
			return 0, err
		}
		if n == 0 && total != 0 {
			break
		}

		shards, encErr := e.EncodeData(buf[:n])
		if encErr != nil {
			return 0, encErr
		}

		ok := 0
		for i, w := range writers {
			if errs[i] != nil || w == nil {
				continue
			}
			if _, errs[i] = w.Write(shards[i]); errs[i] == nil {
				ok++
			}
		}
		if ok < quorum {
			return 0, ErrWriteQuorum
		}

		total += int64(n)
		if eof {
			break
		}
	}
	return total, nil
}

// Decode reads shards from readers and reconstructs the original data into writer.
// offset and length refer to the original (pre-erasure) byte range.
func (e *Erasure) Decode(ctx context.Context, writer io.Writer, readers []io.ReaderAt, offset, length, totalLength int64) error {
	if length == 0 {
		return nil
	}

	shardSize := e.ShardSize()
	startBlock := offset / e.blockSize
	endBlock := (offset + length - 1) / e.blockSize

	for block := startBlock; block <= endBlock; block++ {
		blockStart := block * e.blockSize
		blockEnd := blockStart + e.blockSize
		if blockEnd > totalLength {
			blockEnd = totalLength
		}
		blockLen := blockEnd - blockStart

		shardOff := block * shardSize
		shardLen := e.ShardFileSize(blockLen)

		shards := make([][]byte, len(readers))
		for i, r := range readers {
			if r == nil {
				continue
			}
			shards[i] = make([]byte, shardLen)
			if _, err := r.ReadAt(shards[i], shardOff); err != nil {
				shards[i] = nil
			}
		}

		if err := e.DecodeDataBlocks(shards); err != nil {
			return err
		}

		// Reconstruct original block from data shards
		var decoded []byte
		for i := 0; i < e.dataBlocks; i++ {
			decoded = append(decoded, shards[i]...)
		}
		// Trim to actual block size
		if int64(len(decoded)) > blockLen {
			decoded = decoded[:blockLen]
		}

		// Trim to requested range within this block
		dataStart := offset - blockStart
		if dataStart < 0 {
			dataStart = 0
		}
		dataEnd := offset + length - blockStart
		if dataEnd > int64(len(decoded)) {
			dataEnd = int64(len(decoded))
		}
		if block > startBlock {
			dataStart = 0
		}

		if _, err := writer.Write(decoded[dataStart:dataEnd]); err != nil {
			return err
		}
	}
	return nil
}

func ceilFrac(numerator, denominator int64) int64 {
	if denominator == 0 {
		return 0
	}
	return (numerator + denominator - 1) / denominator
}
