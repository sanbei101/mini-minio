package erasure

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/klauspost/reedsolomon"

	"github.com/sanbei101/mini-minio/internal/bpool"
)

const BlockSize = 10 << 20 // 10 MiB

// Erasure wraps reedsolomon encoder with buffer pool support.
type Erasure struct {
	encoder                  reedsolomon.Encoder
	dataBlocks, parityBlocks int
	blockSize                int64
	pool                     *bpool.BytePoolCap
}

// New creates an Erasure instance. If pool is nil, Encode/Decode will allocate buffers directly.
func New(dataBlocks, parityBlocks int, pool *bpool.BytePoolCap) (Erasure, error) {
	e := Erasure{
		dataBlocks:   dataBlocks,
		parityBlocks: parityBlocks,
		blockSize:    BlockSize,
		pool:         pool,
	}
	enc, err := reedsolomon.New(dataBlocks, parityBlocks,
		reedsolomon.WithAutoGoroutines(int(e.ShardSize())))
	if err != nil {
		return Erasure{}, err
	}
	e.encoder = enc
	return e, nil
}

func (e *Erasure) ShardSize() int64 {
	return e.shardSizeFor(e.blockSize)
}

// shardSizeFor returns the ceil-divided shard size for a block.
func (e *Erasure) shardSizeFor(blockSize int64) int64 {
	divisor := int64(e.dataBlocks)
	if blockSize == 0 || divisor == 0 {
		return 0
	}
	return (blockSize-1)/divisor + 1
}

func (e *Erasure) ShardFileSize(totalLength int64) int64 {
	if totalLength == 0 {
		return 0
	}
	numShards := totalLength / e.blockSize
	lastBlockSize := totalLength % e.blockSize
	lastShardSize := e.shardSizeFor(lastBlockSize)
	return numShards*e.ShardSize() + lastShardSize
}

func (e *Erasure) EncodeData(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return make([][]byte, e.dataBlocks+e.parityBlocks), nil
	}
	shards, err := e.encoder.Split(data)
	if err != nil {
		return nil, err
	}
	if err = e.encoder.Encode(shards); err != nil {
		return nil, err
	}
	return shards, nil
}

func (e *Erasure) DecodeDataBlocks(data [][]byte) error {
	missingData := 0
	allMissing := true
	for i, b := range data {
		if len(b) > 0 {
			allMissing = false
		}
		if i < e.dataBlocks && len(b) == 0 {
			missingData++
		}
	}
	if missingData == 0 || allMissing {
		return nil
	}
	return e.encoder.ReconstructData(data)
}

// --- multiWriter ---

// multiWriter writes erasure shards to multiple disk writers with quorum enforcement.
type multiWriter struct {
	writers     []io.Writer
	writeQuorum int
	errs        []error
}

// Write writes one shard per writer and checks write quorum.
func (mw *multiWriter) Write(blocks [][]byte) error {
	ok := 0
	for i, w := range mw.writers {
		if mw.errs[i] != nil || w == nil {
			continue
		}
		n, err := w.Write(blocks[i])
		if err != nil {
			mw.errs[i] = err
			continue
		}
		if n != len(blocks[i]) {
			mw.errs[i] = io.ErrShortWrite
			continue
		}
		ok++
	}
	if ok < mw.writeQuorum {
		return ErrWriteQuorum
	}
	return nil
}

// Encode reads from src, erasure-encodes each block, and writes shards to writers.
// buf is an externally-provided buffer (from pool) sized to BlockSize.
func (e *Erasure) Encode(
	ctx context.Context,
	src io.Reader,
	writers []io.Writer,
	buf []byte,
	quorum int,
) (int64, error) {
	mw := &multiWriter{
		writers:     writers,
		writeQuorum: quorum,
		errs:        make([]error, len(writers)),
	}

	var total int64
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
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

		if err := mw.Write(shards); err != nil {
			return 0, err
		}

		total += int64(n)
		if eof {
			break
		}
	}
	return total, nil
}

// --- parallelReader ---

// parallelReader reads erasure shards from multiple disks in parallel,
// using a channel-trigger pattern: success stops more reads, failure triggers fallback.
type parallelReader struct {
	readers       []io.ReaderAt
	dataBlocks    int
	offset        int64
	shardSize     int64
	shardFileSize int64
	buf           [][]byte
	readerToBuf   []int
	stashBuffer   []byte // large buffer from pool, sliced into per-shard buffers
}

// newParallelReader creates a parallelReader. If pool is non-nil and large enough,
// it grabs a single buffer from the pool and slices it into per-shard buffers.
func newParallelReader(
	readers []io.ReaderAt,
	e *Erasure,
	offset, totalLength int64,
	pool *bpool.BytePoolCap,
) *parallelReader {
	n := len(readers)
	r2b := make([]int, n)
	for i := range r2b {
		r2b[i] = i
	}

	shardSize := int(e.ShardSize())
	var stash []byte

	// Try to get a single large buffer from pool and slice it.
	if pool != nil && pool.WidthCap() >= n*shardSize {
		stash = pool.Get()
	}

	return &parallelReader{
		readers:       readers,
		dataBlocks:    e.dataBlocks,
		offset:        (offset / e.blockSize) * e.ShardSize(),
		shardSize:     e.ShardSize(),
		shardFileSize: e.ShardFileSize(totalLength),
		buf:           make([][]byte, n),
		readerToBuf:   r2b,
		stashBuffer:   stash,
	}
}

// canDecode returns true if enough shards are available for reconstruction.
func (p *parallelReader) canDecode(buf [][]byte) bool {
	count := 0
	for _, b := range buf {
		if len(b) > 0 {
			count++
		}
	}
	return count >= p.dataBlocks
}

// Read reads one block's worth of shards from disks in parallel.
// Returns the shard buffers (indexed by disk position).
func (p *parallelReader) Read(dst [][]byte) ([][]byte, error) {
	n := len(p.readers)
	newBuf := dst
	if len(dst) != n {
		newBuf = make([][]byte, n)
	} else {
		for i := range newBuf {
			newBuf[i] = newBuf[i][:0]
		}
	}

	shardSize := p.shardSize
	if p.offset+shardSize > p.shardFileSize {
		shardSize = p.shardFileSize - p.offset
	}
	if shardSize == 0 {
		return newBuf, nil
	}

	var mu sync.Mutex
	var disksNotFound atomic.Int32
	readerIndex := 0

	// Channel-trigger: true = try next disk, false = stop.
	readTriggerCh := make(chan bool, n)
	defer close(readTriggerCh)

	// Seed with dataBlocks triggers.
	for i := 0; i < p.dataBlocks; i++ {
		readTriggerCh <- true
	}

	var wg sync.WaitGroup
	for trigger := range readTriggerCh {
		mu.Lock()
		canDecode := p.canDecode(newBuf)
		mu.Unlock()
		if canDecode {
			break
		}
		if readerIndex >= n {
			break
		}
		if !trigger {
			continue
		}

		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			r := p.readers[idx]
			if r == nil {
				readTriggerCh <- true // nil reader, try next
				return
			}

			bufIdx := p.readerToBuf[idx]
			// Allocate or reuse shard buffer.
			if p.buf[bufIdx] == nil {
				if p.stashBuffer != nil && bufIdx < len(p.stashBuffer)/int(shardSize) {
					// Slice from stash buffer.
					start := bufIdx * int(shardSize)
					p.buf[bufIdx] = p.stashBuffer[start : start+int(shardSize)]
				} else {
					p.buf[bufIdx] = make([]byte, shardSize)
				}
			}
			p.buf[bufIdx] = p.buf[bufIdx][:shardSize]

			numRead, err := r.ReadAt(p.buf[bufIdx], p.offset)
			if err != nil {
				p.readers[idx] = nil
				disksNotFound.Add(1)
				readTriggerCh <- true // failure, try next disk
				return
			}

			mu.Lock()
			newBuf[bufIdx] = p.buf[bufIdx][:numRead]
			mu.Unlock()
			readTriggerCh <- false // success, no need for more
		}(readerIndex)
		readerIndex++
	}
	wg.Wait()

	if p.canDecode(newBuf) {
		p.offset += shardSize
		return newBuf, nil
	}
	return nil, fmt.Errorf("%w (offline-disks=%d/%d)", ErrWriteQuorum, disksNotFound.Load(), n)
}

// Decode reads shards from readers in parallel and reconstructs the original data.
// offset and length refer to the original (pre-erasure) byte range.
func (e *Erasure) Decode(
	ctx context.Context,
	writer io.Writer,
	readers []io.ReaderAt,
	offset, length, totalLength int64,
) error {
	if length == 0 {
		return nil
	}

	rp := newParallelReader(readers, e, offset, totalLength, e.pool)
	defer func() {
		if rp.stashBuffer != nil {
			e.pool.Put(rp.stashBuffer)
		}
	}()

	startBlock := offset / e.blockSize
	endBlock := (offset + length - 1) / e.blockSize

	var bufs [][]byte
	for block := startBlock; block <= endBlock; block++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		var err error
		bufs, err = rp.Read(bufs)
		if err != nil {
			return err
		}

		if err := e.DecodeDataBlocks(bufs); err != nil {
			return err
		}

		// Reconstruct original block from data shards.
		blockStart := block * e.blockSize
		blockEnd := min(blockStart+e.blockSize, totalLength)
		blockLen := blockEnd - blockStart

		// Trim to requested range within this block.
		dataStart := max(offset-blockStart, 0)
		dataEnd := min(offset+length-blockStart, blockLen)
		shardSize := int64(len(bufs[0]))
		for i := 0; i < e.dataBlocks; i++ {
			shardStart := int64(i) * shardSize
			shardEnd := min(shardStart+int64(len(bufs[i])), blockLen)
			start := max(dataStart, shardStart)
			end := min(dataEnd, shardEnd)
			if start >= end {
				continue
			}
			if _, err := writer.Write(bufs[i][start-shardStart : end-shardStart]); err != nil {
				return err
			}
		}
	}
	return nil
}
