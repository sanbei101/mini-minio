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
	MakeBucket(context.Context, string) error
	DeleteBucket(context.Context, string) error
	ListBuckets(context.Context) ([]os.FileInfo, error)
	StatBucket(context.Context, string) (os.FileInfo, error)

	CreateShardFile(context.Context, string, string, string, int) (ShardWriter, error)
	ReadShardFile(context.Context, string, string, string, int) (ShardReader, error)
	DeleteObjectData(context.Context, string, string, string) error

	WriteMetaTmp(context.Context, string, string, []byte) error
	RenameMeta(context.Context, string, string) error
	ReadMeta(context.Context, string, string) (io.ReadCloser, error)
	WriteUploadMeta(context.Context, string, string, string, string, []byte) error
	ReadUploadMeta(context.Context, string, string, string, string) (io.ReadCloser, error)
	DeleteUpload(context.Context, string, string, string) error

	DeleteObject(context.Context, string, string) error
	ListObjects(context.Context, string, string) ([]string, error)
}
