package cmd

import (
	"io"

	"github.com/sanbei101/mini-minio/internal/hash"
)

// PutObjReader wraps hash.Reader for upload streams.
type PutObjReader struct {
	*hash.Reader
}

// NewPutObjReader creates a PutObjReader from a plain reader and size.
func NewPutObjReader(r io.Reader, size int64) (*PutObjReader, error) {
	hr, err := hash.NewReader(r, size, "")
	if err != nil {
		return nil, err
	}
	return &PutObjReader{Reader: hr}, nil
}

// GetObjectReader wraps the object stream with its metadata.
type GetObjectReader struct {
	io.Reader
	ObjInfo ObjectInfo
}

// Close closes the underlying reader.
func (g *GetObjectReader) Close() error {
	if rc, ok := g.Reader.(io.Closer); ok {
		return rc.Close()
	}
	return nil
}
