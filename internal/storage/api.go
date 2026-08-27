package storage

import (
	"context"
	"io"
	"os"
)

// ShardWriter receives one erasure shard and commits it when closed.
type ShardWriter interface {
	io.Writer
	io.Closer
}

// ShardReader provides random access to one erasure shard.
type ShardReader interface {
	io.ReaderAt
	io.Closer
}

// API contains the storage operations used by the mini object layer.
// Implementations can be local disks or internal remote-drive clients.
type API interface {
	MakeBucket(string) error
	DeleteBucket(string) error
	ListBuckets() ([]os.FileInfo, error)
	StatBucket(string) (os.FileInfo, error)

	CreateShardFile(context.Context, string, string, string, int) (ShardWriter, error)
	ReadShardFile(context.Context, string, string, string, int) (ShardReader, error)
	DeleteObjectData(string, string, string) error

	WriteMetaTmp(string, string, []byte) error
	RenameMeta(string, string) error
	ReadMeta(string, string) ([]byte, error)
	WriteUploadMeta(string, string, string, string, []byte) error
	ReadUploadMeta(string, string, string, string) ([]byte, error)
	DeleteUpload(string, string, string) error

	DeleteObject(string, string) error
	ListObjects(string, string) ([]string, error)
}
