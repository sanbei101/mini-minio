package storage

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const metaFile = "xl.meta"

var (
	ErrNotFound     = errors.New("not found")
	ErrBucketExists = errors.New("bucket already exists")
)

type Disk struct {
	path string
}

func NewDisk(path string) (*Disk, error) {
	if err := os.MkdirAll(path, 0o755); err != nil {
		return nil, err
	}
	return &Disk{path: path}, nil
}

func (d *Disk) MakeBucket(ctx context.Context, bucket string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := filepath.Join(d.path, bucket)
	if _, err := os.Stat(p); err == nil {
		return ErrBucketExists
	}
	return os.Mkdir(p, 0o755)
}

func (d *Disk) DeleteBucket(ctx context.Context, bucket string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.Remove(filepath.Join(d.path, bucket))
}

func (d *Disk) ListBuckets(ctx context.Context) ([]os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return nil, err
	}
	var infos []os.FileInfo
	for _, e := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if e.IsDir() {
			fi, err := e.Info()
			if err == nil {
				infos = append(infos, fi)
			}
		}
	}
	return infos, nil
}

func (d *Disk) StatBucket(ctx context.Context, bucket string) (os.FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	fi, err := os.Stat(filepath.Join(d.path, bucket))
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	return fi, err
}

// CreateShardFile creates and returns an open file for writing a shard.
func (d *Disk) CreateShardFile(ctx context.Context, bucket, object, dataDir string, partNum int) (ShardWriter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := filepath.Join(d.path, bucket, object, dataDir)
	filePath := filepath.Join(dir, partName(partNum))
	f, err := os.Create(filePath)
	if err == nil {
		return f, nil
	}
	if os.IsNotExist(err) {
		if mkdirErr := os.MkdirAll(dir, 0o755); mkdirErr != nil {
			return nil, mkdirErr
		}
		return os.Create(filePath)
	}
	return nil, err
}

// ReadShardFile returns a ReaderAt for a shard file.
func (d *Disk) ReadShardFile(ctx context.Context, bucket, object, dataDir string, partNum int) (ShardReader, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	p := filepath.Join(d.path, bucket, object, dataDir, partName(partNum))
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (d *Disk) DeleteObjectData(ctx context.Context, bucket, object, dataDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(d.path, bucket, object, dataDir))
}

// WriteMetaTmp writes metadata to a temporary file. Returns the tmp path on success.
func (d *Disk) WriteMetaTmp(ctx context.Context, bucket, object string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := filepath.Join(d.path, bucket, object)
	tmp := filepath.Join(dir, metaFile+".tmp")
	err := os.WriteFile(tmp, data, 0o644)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		if mkdirErr := os.MkdirAll(dir, 0o755); mkdirErr != nil {
			return mkdirErr
		}
		return os.WriteFile(tmp, data, 0o644)
	}
	return err
}

// RenameMeta atomically renames the temp metadata file to the final xl.meta.
func (d *Disk) RenameMeta(ctx context.Context, bucket, object string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := filepath.Join(d.path, bucket, object)
	tmp := filepath.Join(dir, metaFile+".tmp")
	dst := filepath.Join(dir, metaFile)
	return os.Rename(tmp, dst)
}

func (d *Disk) ReadMeta(ctx context.Context, bucket, object string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(d.path, bucket, object, metaFile))
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (d *Disk) WriteUploadMeta(ctx context.Context, bucket, object, uploadID, name string, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := d.uploadDir(bucket, object, uploadID)
	target := filepath.Join(dir, name+".json")
	err := os.WriteFile(target, data, 0o644)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) {
		if mkdirErr := os.MkdirAll(dir, 0o755); mkdirErr != nil {
			return mkdirErr
		}
		return os.WriteFile(target, data, 0o644)
	}
	return err
}

func (d *Disk) ReadUploadMeta(ctx context.Context, bucket, object, uploadID, name string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f, err := os.Open(filepath.Join(d.uploadDir(bucket, object, uploadID), name+".json"))
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (d *Disk) DeleteUpload(ctx context.Context, bucket, object, uploadID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.RemoveAll(d.uploadDir(bucket, object, uploadID))
}

func (d *Disk) DeleteObject(ctx context.Context, bucket, object string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Join(d.path, bucket, object))
}

func (d *Disk) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	dir := filepath.Join(d.path, bucket)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}

	walkDir := dir
	if slash := strings.LastIndexByte(prefix, '/'); slash >= 0 {
		candidate := filepath.FromSlash(prefix[:slash+1])
		if !filepath.IsAbs(candidate) {
			candidateDir := filepath.Join(dir, candidate)
			rel, relErr := filepath.Rel(dir, candidateDir)
			if relErr == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				walkDir = candidateDir
				if _, err := os.Stat(walkDir); os.IsNotExist(err) {
					return []string{}, nil
				} else if err != nil {
					return nil, err
				}
			}
		}
	}

	names := make([]string, 0)
	err := filepath.WalkDir(walkDir, func(path string, entry os.DirEntry, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() != metaFile {
			return nil
		}
		objectDir := filepath.Dir(path)
		name, err := filepath.Rel(dir, objectDir)
		if err != nil {
			return err
		}
		name = filepath.ToSlash(name)
		if prefix == "" || len(name) >= len(prefix) && name[:len(prefix)] == prefix {
			names = append(names, name)
		}
		return nil
	})
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return names, nil
}

func partName(n int) string {
	return "part." + strconv.Itoa(n)
}

func (d *Disk) uploadDir(bucket, object, uploadID string) string {
	return filepath.Join(d.path, bucket, object, ".multipart", uploadID)
}
