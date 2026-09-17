package storage

import (
	"context"
	"errors"
	"io"
	"sync"
)

// NewBufferedShardWriter decouples erasure encoding from one drive's I/O.
// Capacity is counted in complete erasure blocks.
func NewBufferedShardWriter(ctx context.Context, dst ShardWriter, capacity int) ShardWriter {
	w := &bufferedShardWriter{
		ctx:    ctx,
		dst:    dst,
		chunks: make(chan *[]byte, capacity),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

type bufferedShardWriter struct {
	ctx    context.Context
	dst    ShardWriter
	chunks chan *[]byte
	done   chan struct{}

	stateMu sync.Mutex
	errMu   sync.Mutex
	err     error
	closed  bool
	once    sync.Once
}

var chunkPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1<<20)
		return &b
	},
}

func getChunkBuffer(size int) *[]byte {
	bp, ok := chunkPool.Get().(*[]byte)
	if !ok || cap(*bp) < size {
		b := make([]byte, size)
		return &b
	}
	*bp = (*bp)[:size]
	return bp
}

func putChunkBuffer(bp *[]byte) {
	if bp != nil && cap(*bp) >= 64<<10 && cap(*bp) <= 8<<20 {
		*bp = (*bp)[:0]
		chunkPool.Put(bp)
	}
}

func (w *bufferedShardWriter) Write(p []byte) (int, error) {
	w.stateMu.Lock()
	defer w.stateMu.Unlock()
	if w.closed {
		return 0, errors.New("write after close")
	}
	w.errMu.Lock()
	err := w.err
	w.errMu.Unlock()
	if err != nil {
		return 0, err
	}
	chunk := getChunkBuffer(len(p))
	copy(*chunk, p)
	select {
	case <-w.ctx.Done():
		putChunkBuffer(chunk)
		return 0, w.ctx.Err()
	case w.chunks <- chunk:
		return len(p), nil
	}
}

func (w *bufferedShardWriter) Close() error {
	w.once.Do(func() {
		w.stateMu.Lock()
		w.closed = true
		close(w.chunks)
		w.stateMu.Unlock()
		select {
		case <-w.done:
		case <-w.ctx.Done():
			w.errMu.Lock()
			if w.err == nil {
				w.err = w.ctx.Err()
			}
			w.errMu.Unlock()
		}
	})
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.err
}

func (w *bufferedShardWriter) run() {
	for chunk := range w.chunks {
		_, err := w.dst.Write(*chunk)
		putChunkBuffer(chunk)
		if err == nil {
			continue
		}
		w.errMu.Lock()
		w.err = err
		w.errMu.Unlock()
	}
	if err := w.dst.Close(); err != nil {
		w.errMu.Lock()
		if w.err == nil {
			w.err = err
		}
		w.errMu.Unlock()
	}
	close(w.done)
}

var _ io.WriteCloser = (*bufferedShardWriter)(nil)
