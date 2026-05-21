package cmd

import (
	"io"
	"sync"

	"github.com/sanbei101/mini-minio/internal/hash"
)

// PutObjReader is a type that wraps sio.EncryptReader and
// underlying hash.Reader in a struct
type PutObjReader struct {
	*hash.Reader // data stream
}

// GetObjectReader is a type that wraps a reader with a lock to
// provide a ReadCloser interface that unlocks on Close()
type GetObjectReader struct {
	io.Reader
	ObjInfo    ObjectInfo
	cleanUpFns []func()
	once       sync.Once
}
