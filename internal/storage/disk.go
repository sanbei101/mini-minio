package storage

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
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

func (d *Disk) Path() string { return d.path }

func (d *Disk) MakeBucket(bucket string) error {
	p := filepath.Join(d.path, bucket)
	if _, err := os.Stat(p); err == nil {
		return ErrBucketExists
	}
	return os.Mkdir(p, 0o755)
}

func (d *Disk) DeleteBucket(bucket string) error {
	return os.Remove(filepath.Join(d.path, bucket))
}

func (d *Disk) ListBuckets() ([]os.FileInfo, error) {
	entries, err := os.ReadDir(d.path)
	if err != nil {
		return nil, err
	}
	var infos []os.FileInfo
	for _, e := range entries {
		if e.IsDir() {
			fi, err := e.Info()
			if err == nil {
				infos = append(infos, fi)
			}
		}
	}
	return infos, nil
}

func (d *Disk) StatBucket(bucket string) (os.FileInfo, error) {
	fi, err := os.Stat(filepath.Join(d.path, bucket))
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	return fi, err
}

// CreateShardFile creates and returns an open file for writing a shard.
func (d *Disk) CreateShardFile(bucket, object, dataDir string, partNum int) (*os.File, error) {
	dir := filepath.Join(d.path, bucket, object, dataDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.Create(filepath.Join(dir, partName(partNum)))
}

// ReadShardFile returns a ReaderAt for a shard file.
func (d *Disk) ReadShardFile(bucket, object, dataDir string, partNum int) (io.ReadCloser, int64, error) {
	p := filepath.Join(d.path, bucket, object, dataDir, partName(partNum))
	f, err := os.Open(p)
	if os.IsNotExist(err) {
		return nil, 0, ErrNotFound
	}
	if err != nil {
		return nil, 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	return f, fi.Size(), nil
}

func (d *Disk) WriteMeta(bucket, object string, meta any) error {
	dir := filepath.Join(d.path, bucket, object)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, metaFile), data, 0o644)
}

// WriteMetaTmp writes metadata to a temporary file. Returns the tmp path on success.
func (d *Disk) WriteMetaTmp(bucket, object string, meta any) (string, error) {
	dir := filepath.Join(d.path, bucket, object)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	data, err := json.Marshal(meta)
	if err != nil {
		return "", err
	}
	tmp := filepath.Join(dir, metaFile+".tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return "", err
	}
	return tmp, nil
}

// RenameMeta atomically renames the temp metadata file to the final xl.meta.
func (d *Disk) RenameMeta(bucket, object string) error {
	dir := filepath.Join(d.path, bucket, object)
	tmp := filepath.Join(dir, metaFile+".tmp")
	dst := filepath.Join(dir, metaFile)
	return os.Rename(tmp, dst)
}

func (d *Disk) ReadMeta(bucket, object string, out any) error {
	data, err := os.ReadFile(filepath.Join(d.path, bucket, object, metaFile))
	if os.IsNotExist(err) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return json.Unmarshal(data, out)
}

func (d *Disk) DeleteObject(bucket, object string) error {
	return os.RemoveAll(filepath.Join(d.path, bucket, object))
}

func (d *Disk) ListObjects(bucket, prefix string) ([]string, error) {
	dir := filepath.Join(d.path, bucket)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return nil, ErrNotFound
	} else if err != nil {
		return nil, err
	}

	var names []string
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
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
	return "part." + itoa(n)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := [20]byte{}
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
