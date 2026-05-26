package cmd

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sanbei101/mini-minio/internal/erasure"
	"github.com/sanbei101/mini-minio/internal/storage"
)

// xlMeta is the per-object metadata stored as xl.meta on each disk.
type xlMeta struct {
	Name         string            `json:"name"`
	Bucket       string            `json:"bucket"`
	Size         int64             `json:"size"`
	ModTime      time.Time         `json:"modTime"`
	ETag         string            `json:"etag"`
	ContentType  string            `json:"contentType"`
	DataDir      string            `json:"dataDir"`
	DataBlocks   int               `json:"dataBlocks"`
	ParityBlocks int               `json:"parityBlocks"`
	BlockSize    int64             `json:"blockSize"`
	Parts        []ObjectPartInfo  `json:"parts"`
	UserMeta     map[string]string `json:"userMeta,omitempty"`
	DiskIndex    int               `json:"diskIndex"`
}

// erasureObjects implements ObjectLayer using erasure coding across multiple disks.
type erasureObjects struct {
	disks        []*storage.Disk
	dataBlocks   int
	parityBlocks int
	mu           sync.RWMutex
}

// NewErasureObjects creates an ObjectLayer backed by the given disk paths.
func NewErasureObjects(diskPaths []string, dataBlocks, parityBlocks int) (ObjectLayer, error) {
	return NewErasureSets(diskPaths, dataBlocks, parityBlocks)
}

func newSingleErasureObjects(diskPaths []string, dataBlocks, parityBlocks int) (*erasureObjects, error) {
	if len(diskPaths) != dataBlocks+parityBlocks {
		return nil, fmt.Errorf("need %d disk paths, got %d", dataBlocks+parityBlocks, len(diskPaths))
	}
	disks := make([]*storage.Disk, len(diskPaths))
	for i, p := range diskPaths {
		d, err := storage.NewDisk(p)
		if err != nil {
			return nil, err
		}
		disks[i] = d
	}
	return &erasureObjects{
		disks:        disks,
		dataBlocks:   dataBlocks,
		parityBlocks: parityBlocks,
	}, nil
}

func (e *erasureObjects) MakeBucket(ctx context.Context, bucket string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	exists := 0
	for _, d := range e.disks {
		if err := d.MakeBucket(bucket); err != nil {
			if errors.Is(err, storage.ErrBucketExists) {
				exists++
				continue
			}
			return err
		}
	}
	if exists == len(e.disks) {
		return ErrBucketExists
	}
	return nil
}

func (e *erasureObjects) GetBucketInfo(ctx context.Context, bucket string) (BucketInfo, error) {
	fi, err := e.disks[0].StatBucket(bucket)
	if errors.Is(err, storage.ErrNotFound) {
		return BucketInfo{}, ErrBucketNotFound
	}
	if err != nil {
		return BucketInfo{}, err
	}
	return BucketInfo{Name: bucket, Created: fi.ModTime()}, nil
}

func (e *erasureObjects) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	infos, err := e.disks[0].ListBuckets()
	if err != nil {
		return nil, err
	}
	buckets := make([]BucketInfo, len(infos))
	for i, fi := range infos {
		buckets[i] = BucketInfo{Name: fi.Name(), Created: fi.ModTime()}
	}
	return buckets, nil
}

func (e *erasureObjects) DeleteBucket(ctx context.Context, bucket string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	missing := 0
	for _, d := range e.disks {
		if err := d.DeleteBucket(bucket); err != nil {
			if os.IsNotExist(err) {
				missing++
				continue
			}
			return err
		}
	}
	if missing == len(e.disks) {
		return ErrBucketNotFound
	}
	return nil
}

func (e *erasureObjects) PutObject(ctx context.Context, bucket, object string, data *PutObjReader) (ObjectInfo, error) {
	enc, err := erasure.New(e.dataBlocks, e.parityBlocks)
	if err != nil {
		return ObjectInfo{}, err
	}

	dataDir := uuid.New().String()
	writers := make([]io.Writer, len(e.disks))
	files := make([]*os.File, len(e.disks))

	for i, d := range e.disks {
		f, ferr := d.CreateShardFile(bucket, object, dataDir, 1)
		if ferr != nil {
			for j := 0; j < i; j++ {
				files[j].Close()
			}
			return ObjectInfo{}, ferr
		}
		files[i] = f
		writers[i] = f
	}

	writeQuorum := e.dataBlocks
	if e.dataBlocks == e.parityBlocks {
		writeQuorum++
	}

	md5h := md5.New()
	tee := io.TeeReader(data, md5h)

	n, encErr := enc.Encode(ctx, tee, writers, writeQuorum)
	for _, f := range files {
		f.Close()
	}
	if encErr != nil {
		return ObjectInfo{}, encErr
	}

	etag := hex.EncodeToString(md5h.Sum(nil))
	now := time.Now().UTC()
	contentType := "application/octet-stream"

	meta := xlMeta{
		Name:         object,
		Bucket:       bucket,
		Size:         n,
		ModTime:      now,
		ETag:         etag,
		ContentType:  contentType,
		DataDir:      dataDir,
		DataBlocks:   e.dataBlocks,
		ParityBlocks: e.parityBlocks,
		BlockSize:    erasure.BlockSize,
		Parts:        []ObjectPartInfo{{Number: 1, Size: enc.ShardFileSize(n), ActualSize: n}},
	}

	for i, d := range e.disks {
		m := meta
		m.DiskIndex = i
		if err := d.WriteMeta(bucket, object, &m); err != nil {
			return ObjectInfo{}, err
		}
	}

	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        n,
		ModTime:     now,
		ETag:        etag,
		ContentType: contentType,
	}, nil
}

func (e *erasureObjects) GetObjectInfo(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	meta, err := e.readMeta(bucket, object)
	if err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        meta.Size,
		ModTime:     meta.ModTime,
		ETag:        meta.ETag,
		ContentType: meta.ContentType,
	}, nil
}

func (e *erasureObjects) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header) (*GetObjectReader, error) {
	meta, err := e.readMeta(bucket, object)
	if err != nil {
		return nil, err
	}

	enc, err := erasure.New(meta.DataBlocks, meta.ParityBlocks)
	if err != nil {
		return nil, err
	}

	readers := make([]io.ReaderAt, len(e.disks))
	closers := make([]io.Closer, len(e.disks))
	for i, d := range e.disks {
		rc, _, ferr := d.ReadShardFile(bucket, object, meta.DataDir, 1)
		if ferr == nil {
			if rat, ok := rc.(io.ReaderAt); ok {
				readers[i] = rat
				closers[i] = rc
			}
		}
	}

	offset, length := int64(0), meta.Size
	if rs != nil {
		offset, length, err = rs.GetOffsetLength(meta.Size)
		if err != nil {
			return nil, err
		}
	}

	pr, pw := io.Pipe()
	go func() {
		decErr := enc.Decode(ctx, pw, readers, offset, length, meta.Size)
		pw.CloseWithError(decErr)
		for _, c := range closers {
			if c != nil {
				c.Close()
			}
		}
	}()

	objInfo := ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        meta.Size,
		ModTime:     meta.ModTime,
		ETag:        meta.ETag,
		ContentType: meta.ContentType,
	}
	return &GetObjectReader{Reader: pr, ObjInfo: objInfo}, nil
}

func (e *erasureObjects) DeleteObject(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	info, err := e.GetObjectInfo(ctx, bucket, object)
	if err != nil {
		return ObjectInfo{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, d := range e.disks {
		_ = d.DeleteObject(bucket, object)
	}
	return info, nil
}

func (e *erasureObjects) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
	objects, err := e.listObjectInfos(ctx, bucket, prefix)
	if err != nil {
		return ListObjectsV2Info{}, err
	}
	return buildListObjectsV2Result(objects, prefix, continuationToken, delimiter, maxKeys, startAfter), nil
}

func (e *erasureObjects) listObjectInfos(ctx context.Context, bucket, prefix string) ([]ObjectInfo, error) {
	names, err := e.disks[0].ListObjects(bucket, prefix)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, ErrBucketNotFound
	}
	if err != nil {
		return nil, err
	}

	sort.Strings(names)
	objects := make([]ObjectInfo, 0, len(names))
	for _, name := range names {
		meta, merr := e.readMeta(bucket, name)
		if merr != nil {
			continue
		}
		objects = append(objects, ObjectInfo{
			Bucket:  bucket,
			Name:    name,
			Size:    meta.Size,
			ModTime: meta.ModTime,
			ETag:    meta.ETag,
		})
	}
	return objects, nil
}

func buildListObjectsV2Result(objects []ObjectInfo, prefix, continuationToken, delimiter string, maxKeys int, startAfter string) ListObjectsV2Info {
	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	start := startAfter
	if continuationToken != "" {
		start = continuationToken
	}
	if start != "" {
		i := sort.Search(len(objects), func(i int) bool {
			return objects[i].Name >= start
		})
		if i < len(objects) && objects[i].Name == start {
			i++
		}
		objects = objects[i:]
	}

	result := ListObjectsV2Info{}
	prefixes := make([]string, 0)
	seen := make(map[string]bool)

	for i, object := range objects {
		if len(result.Objects)+len(prefixes) >= maxKeys {
			result.IsTruncated = true
			if i > 0 {
				result.NextContinuationToken = objects[i-1].Name
			}
			break
		}
		if delimiter != "" {
			rel := strings.TrimPrefix(object.Name, prefix)
			if idx := strings.Index(rel, delimiter); idx >= 0 {
				cp := prefix + rel[:idx+len(delimiter)]
				if !seen[cp] {
					seen[cp] = true
					prefixes = append(prefixes, cp)
				}
				continue
			}
		}
		result.Objects = append(result.Objects, object)
	}

	result.Prefixes = prefixes
	return result
}

func (e *erasureObjects) readMeta(bucket, object string) (*xlMeta, error) {
	var meta xlMeta
	for _, d := range e.disks {
		if err := d.ReadMeta(bucket, object, &meta); err == nil {
			return &meta, nil
		}
	}
	return nil, ErrObjectNotFound
}

// GetOffsetLength resolves the range spec against the object size.
func (rs *HTTPRangeSpec) GetOffsetLength(size int64) (int64, int64, error) {
	if rs.IsSuffixLength {
		start := size + rs.Start
		if start < 0 {
			start = 0
		}
		return start, size - start, nil
	}
	start := rs.Start
	end := rs.End
	if end < 0 || end >= size {
		end = size - 1
	}
	if start > end {
		return 0, 0, fmt.Errorf("invalid range")
	}
	return start, end - start + 1, nil
}

// --- Multipart upload ---

type multipartUpload struct {
	bucket string
	object string
	id     string
	parts  map[int][]byte
	etags  map[int]string
	mu     sync.Mutex
}

var (
	multipartMu      sync.Mutex
	multipartUploads = map[string]*multipartUpload{}
)

func newMultipartUpload(bucket, object string) string {
	id := uuid.New().String()
	multipartMu.Lock()
	multipartUploads[id] = &multipartUpload{
		bucket: bucket,
		object: object,
		id:     id,
		parts:  map[int][]byte{},
		etags:  map[int]string{},
	}
	multipartMu.Unlock()
	return id
}

func uploadPart(uploadID string, partNumber int, r io.Reader) (string, error) {
	multipartMu.Lock()
	up, ok := multipartUploads[uploadID]
	multipartMu.Unlock()
	if !ok {
		return "", fmt.Errorf("upload not found: %s", uploadID)
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return "", err
	}
	h := md5.Sum(data)
	etag := hex.EncodeToString(h[:])

	up.mu.Lock()
	up.parts[partNumber] = data
	up.etags[partNumber] = etag
	up.mu.Unlock()
	return etag, nil
}

func completeMultipartUpload(ctx context.Context, ol ObjectLayer, uploadID string, partNumbers []int) (ObjectInfo, error) {
	multipartMu.Lock()
	up, ok := multipartUploads[uploadID]
	multipartMu.Unlock()
	if !ok {
		return ObjectInfo{}, fmt.Errorf("upload not found: %s", uploadID)
	}

	up.mu.Lock()
	var buf bytes.Buffer
	for _, n := range partNumbers {
		p, exists := up.parts[n]
		if !exists {
			up.mu.Unlock()
			return ObjectInfo{}, fmt.Errorf("part %d not found", n)
		}
		buf.Write(p)
	}
	up.mu.Unlock()

	r, err := NewPutObjReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		return ObjectInfo{}, err
	}
	info, err := ol.PutObject(ctx, up.bucket, up.object, r)
	if err != nil {
		return ObjectInfo{}, err
	}

	multipartMu.Lock()
	delete(multipartUploads, uploadID)
	multipartMu.Unlock()
	return info, nil
}

func abortMultipartUpload(uploadID string) {
	multipartMu.Lock()
	delete(multipartUploads, uploadID)
	multipartMu.Unlock()
}
