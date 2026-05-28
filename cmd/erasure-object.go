package cmd

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/phuslu/log"

	"github.com/sanbei101/mini-minio/internal/bpool"
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
	pool         *bpool.BytePoolCap
	mu           sync.RWMutex
}

func newErasureObjects(
	diskPaths []string,
	dataBlocks, parityBlocks int,
	pool *bpool.BytePoolCap,
) (*erasureObjects, error) {
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
		pool:         pool,
	}, nil
}

func (e *erasureObjects) statBucket(bucket string) (os.FileInfo, error) {
	var firstErr error
	for _, disk := range e.disks {
		info, err := disk.StatBucket(bucket)
		if err == nil {
			return info, nil
		}
		if !errors.Is(err, storage.ErrNotFound) && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, storage.ErrNotFound
}

func (e *erasureObjects) listBucketInfos() ([]BucketInfo, error) {
	type result struct {
		infos []BucketInfo
		err   error
	}
	results := make([]result, len(e.disks))
	var wg sync.WaitGroup
	for i, disk := range e.disks {
		wg.Add(1)
		go func(idx int, d *storage.Disk) {
			defer wg.Done()
			infos, err := d.ListBuckets()
			if err != nil {
				results[idx] = result{err: err}
				return
			}
			buckets := make([]BucketInfo, 0, len(infos))
			for _, info := range infos {
				buckets = append(buckets, BucketInfo{
					Name:    info.Name(),
					Created: info.ModTime(),
				})
			}
			results[idx] = result{infos: buckets}
		}(i, disk)
	}
	wg.Wait()

	bucketByName := map[string]BucketInfo{}
	var firstErr error
	var okDisks int
	for _, r := range results {
		if r.err != nil {
			if firstErr == nil {
				firstErr = r.err
			}
			continue
		}
		okDisks++
		for _, b := range r.infos {
			existing, exists := bucketByName[b.Name]
			if !exists || b.Created.Before(existing.Created) {
				bucketByName[b.Name] = b
			}
		}
	}
	if okDisks == 0 && firstErr != nil {
		return nil, firstErr
	}

	buckets := make([]BucketInfo, 0, len(bucketByName))
	for _, b := range bucketByName {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})
	return buckets, nil
}

func (e *erasureObjects) listObjectNames(bucket, prefix string) ([]string, error) {
	seen := map[string]bool{}
	names := []string{}
	var firstErr error
	var foundBucket bool

	for _, disk := range e.disks {
		diskNames, err := disk.ListObjects(bucket, prefix)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		foundBucket = true
		for _, name := range diskNames {
			if seen[name] {
				continue
			}
			seen[name] = true
			names = append(names, name)
		}
	}
	if !foundBucket {
		if firstErr != nil {
			return nil, firstErr
		}
		return nil, storage.ErrNotFound
	}

	sort.Strings(names)
	return names, nil
}

func (e *erasureObjects) MakeBucket(ctx context.Context, bucket string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	errs := make([]error, len(e.disks))
	var wg sync.WaitGroup
	for i, d := range e.disks {
		wg.Add(1)
		go func(idx int, disk *storage.Disk) {
			defer wg.Done()
			err := disk.MakeBucket(bucket)
			if errors.Is(err, storage.ErrBucketExists) {
				err = nil
			}
			errs[idx] = err
		}(i, d)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *erasureObjects) GetBucketInfo(ctx context.Context, bucket string) (BucketInfo, error) {
	fi, err := e.statBucket(bucket)
	if errors.Is(err, storage.ErrNotFound) {
		return BucketInfo{}, ErrBucketNotFound
	}
	if err != nil {
		return BucketInfo{}, err
	}
	return BucketInfo{Name: bucket, Created: fi.ModTime()}, nil
}

func (e *erasureObjects) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	return e.listBucketInfos()
}

func (e *erasureObjects) DeleteBucket(ctx context.Context, bucket string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	var wg sync.WaitGroup
	errs := make([]error, len(e.disks))
	for i, d := range e.disks {
		wg.Add(1)
		go func(idx int, disk *storage.Disk) {
			defer wg.Done()
			err := disk.DeleteBucket(bucket)
			if os.IsNotExist(err) {
				err = nil
			}
			errs[idx] = err
		}(i, d)
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *erasureObjects) PutObject(ctx context.Context, bucket, object string, data *PutObjReader) (ObjectInfo, error) {
	enc, err := erasure.New(e.dataBlocks, e.parityBlocks, e.pool)
	if err != nil {
		return ObjectInfo{}, err
	}

	dataDir := uuid.New().String()
	writers := make([]io.Writer, len(e.disks))
	files := make([]*os.File, len(e.disks))

	for i, d := range e.disks {
		f, ferr := d.CreateShardFile(bucket, object, dataDir, 1)
		if ferr != nil {
			for j := range i {
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

	// Get buffer from pool.
	var buffer []byte
	if e.pool != nil {
		buffer = e.pool.Get()
		defer e.pool.Put(buffer)
	} else {
		buffer = make([]byte, erasure.BlockSize)
	}
	buffer = buffer[:erasure.BlockSize]

	md5h := md5.New()
	tee := io.TeeReader(data, md5h)

	n, encErr := enc.Encode(ctx, tee, writers, buffer, writeQuorum)
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

	// Write-then-rename: write tmp files in parallel, then rename atomically.
	tmpPaths := make([]string, len(e.disks))
	var wg sync.WaitGroup
	metaErrs := make([]error, len(e.disks))
	for i, d := range e.disks {
		wg.Add(1)
		go func(idx int, disk *storage.Disk) {
			defer wg.Done()
			m := meta
			m.DiskIndex = idx
			tmp, err := disk.WriteMetaTmp(bucket, object, &m)
			if err != nil {
				metaErrs[idx] = err
				return
			}
			tmpPaths[idx] = tmp
		}(i, d)
	}
	wg.Wait()

	writeOK := 0
	for _, err := range metaErrs {
		if err == nil {
			writeOK++
		}
	}
	if writeOK < writeQuorum {
		// Cleanup tmp files.
		for _, tmp := range tmpPaths {
			if tmp != "" {
				os.Remove(tmp)
			}
		}
		return ObjectInfo{}, fmt.Errorf("metadata write quorum not met (%d/%d)", writeOK, writeQuorum)
	}

	// Rename tmp -> final in parallel.
	var renameWg sync.WaitGroup
	renameErrs := make([]error, len(e.disks))
	for i, d := range e.disks {
		if tmpPaths[i] == "" {
			continue
		}
		renameWg.Add(1)
		go func(idx int, disk *storage.Disk) {
			defer renameWg.Done()
			renameErrs[idx] = disk.RenameMeta(bucket, object)
		}(i, d)
	}
	renameWg.Wait()

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

func (e *erasureObjects) GetObjectNInfo(
	ctx context.Context,
	bucket, object string,
	rs *HTTPRangeSpec,
) (*GetObjectReader, error) {
	meta, err := e.readMeta(bucket, object)
	if err != nil {
		return nil, err
	}

	enc, err := erasure.New(meta.DataBlocks, meta.ParityBlocks, e.pool)
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
	disks := e.disks
	e.mu.Unlock()

	errs := make([]error, len(disks))
	var wg sync.WaitGroup
	for i, d := range disks {
		wg.Add(1)
		go func(idx int, disk *storage.Disk) {
			defer wg.Done()

			if ctx.Err() != nil {
				errs[idx] = ctx.Err()
				return
			}
			if disk == nil {
				errs[idx] = errors.New("disk not found")
				return
			}
			errs[idx] = disk.DeleteObject(bucket, object)
		}(i, d)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ObjectInfo{}, ctx.Err()
	case <-done:
	}

	var failCount int
	for index, err := range errs {
		if err != nil {
			failCount++
			// 记录下哪些盘失败了
			log.Error().
				Err(err).
				Str("bucket", bucket).
				Str("object", object).
				Int("diskIndex", index).
				Msg("disk delete failed")
			continue
		}
	}
	writeQuorum := len(disks)/2 + 1
	successCount := len(disks) - failCount

	if successCount < writeQuorum {
		return ObjectInfo{}, fmt.Errorf("delete failed: only %d/%d disks succeeded", successCount, len(disks))
	}

	return info, nil
}

func (e *erasureObjects) ListObjectsV2(
	ctx context.Context,
	bucket, prefix, continuationToken, delimiter string,
	maxKeys int,
	startAfter string,
) (ListObjectsV2Info, error) {
	names, err := e.listObjectNames(bucket, prefix)
	if errors.Is(err, storage.ErrNotFound) {
		return ListObjectsV2Info{}, ErrBucketNotFound
	}
	if err != nil {
		return ListObjectsV2Info{}, err
	}

	start := startAfter
	if continuationToken != "" {
		start = continuationToken
	}
	if start != "" {
		i := sort.SearchStrings(names, start)
		if i < len(names) && names[i] == start {
			i++
		}
		names = names[i:]
	}

	if maxKeys <= 0 || maxKeys > 1000 {
		maxKeys = 1000
	}

	var objects []ObjectInfo
	var prefixes []string
	seen := map[string]bool{}

	for _, name := range names {
		if len(objects)+len(prefixes) >= maxKeys {
			break
		}
		if delimiter != "" {
			rel := strings.TrimPrefix(name, prefix)
			if idx := strings.Index(rel, delimiter); idx >= 0 {
				cp := prefix + rel[:idx+len(delimiter)]
				if !seen[cp] {
					seen[cp] = true
					prefixes = append(prefixes, cp)
				}
				continue
			}
		}
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

	result := ListObjectsV2Info{Objects: objects, Prefixes: prefixes}
	if len(objects)+len(prefixes) >= maxKeys && len(names) > maxKeys {
		result.IsTruncated = true
		if len(objects) > 0 {
			result.NextContinuationToken = objects[len(objects)-1].Name
		}
	}
	return result, nil
}

// readMeta reads xl.meta from all disks in parallel and picks the best via quorum.
func (e *erasureObjects) readMeta(bucket, object string) (*xlMeta, error) {
	metas := make([]*xlMeta, len(e.disks))
	errs := make([]error, len(e.disks))
	var wg sync.WaitGroup
	for i, d := range e.disks {
		wg.Add(1)
		go func(idx int, disk *storage.Disk) {
			defer wg.Done()
			var m xlMeta
			if err := disk.ReadMeta(bucket, object, &m); err != nil {
				errs[idx] = err
				return
			}
			metas[idx] = &m
		}(i, d)
	}
	wg.Wait()

	// Quorum vote: pick the meta that appears most (by ETag + ModTime match).
	type metaKey struct {
		etag    string
		modTime time.Time
	}
	counts := map[metaKey]int{}
	bestMeta := metas[0]
	for _, m := range metas {
		if m == nil {
			continue
		}
		k := metaKey{etag: m.ETag, modTime: m.ModTime}
		counts[k]++
	}

	var bestCount int
	for _, m := range metas {
		if m == nil {
			continue
		}
		k := metaKey{etag: m.ETag, modTime: m.ModTime}
		if counts[k] > bestCount {
			bestCount = counts[k]
			bestMeta = m
		}
	}
	if bestMeta == nil {
		return nil, ErrObjectNotFound
	}
	return bestMeta, nil
}

// GetOffsetLength resolves the range spec against the object size.
func (rs *HTTPRangeSpec) GetOffsetLength(size int64) (int64, int64, error) {
	if rs.IsSuffixLength {
		start := max(size+rs.Start, 0)
		return start, size - start, nil
	}
	start := rs.Start
	end := rs.End
	if end < 0 || end >= size {
		end = size - 1
	}
	if start > end {
		return 0, 0, errors.New("invalid range")
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

func completeMultipartUpload(
	ctx context.Context,
	ol ObjectLayer,
	uploadID string,
	partNumbers []int,
) (ObjectInfo, error) {
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
