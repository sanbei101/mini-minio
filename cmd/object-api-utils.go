package cmd

import (
	"io"
	"sync"

	"github.com/sanbei101/mini-minio/internal/hash"
)

// PutObjReader wraps hash.Reader for upload streams.
type PutObjReader struct {
	*hash.Reader
}

// NewPutObjReader creates a PutObjReader from a plain reader and size.
func NewPutObjReader(r io.Reader, size int64) (*PutObjReader, error) {
	hr, err := hash.NewReader(r, size, "", "", size)
	if err != nil {
		return nil, err
	}
	return &PutObjReader{Reader: hr}, nil
}

// GetObjectReader wraps a reader with cleanup functions.
type GetObjectReader struct {
	io.Reader
	ObjInfo    ObjectInfo
	cleanUpFns []func()
	once       sync.Once
}

// Close runs cleanup functions and closes the underlying reader.
func (g *GetObjectReader) Close() error {
	g.once.Do(func() {
		for _, fn := range g.cleanUpFns {
			fn()
		}
	})
	if rc, ok := g.Reader.(io.Closer); ok {
		return rc.Close()
	}
	return nil
}
