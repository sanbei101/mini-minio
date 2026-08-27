package storage

import (
	"errors"
	"io"
	"sync"
)

// NewBufferedShardWriter decouples erasure encoding from one drive's I/O.
// Capacity is counted in complete erasure blocks.
func NewBufferedShardWriter(dst ShardWriter, capacity int) ShardWriter {
	w := &bufferedShardWriter{
		dst:    dst,
		chunks: make(chan []byte, capacity),
		done:   make(chan struct{}),
	}
	go w.run()
	return w
}

type bufferedShardWriter struct {
	dst    ShardWriter
	chunks chan []byte
	done   chan struct{}

	stateMu sync.Mutex
	errMu   sync.Mutex
	err     error
	closed  bool
	once    sync.Once
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
	chunk := append([]byte(nil), p...)
	w.chunks <- chunk
	return len(p), nil
}

func (w *bufferedShardWriter) Close() error {
	w.once.Do(func() {
		w.stateMu.Lock()
		w.closed = true
		close(w.chunks)
		w.stateMu.Unlock()
		<-w.done
	})
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.err
}

func (w *bufferedShardWriter) run() {
	for chunk := range w.chunks {
		_, err := w.dst.Write(chunk)
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
