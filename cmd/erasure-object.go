package cmd

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/maphash"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
	"uuid"

	"github.com/phuslu/log"

	"github.com/sanbei101/mini-minio/internal/bpool"
	"github.com/sanbei101/mini-minio/internal/erasure"
	"github.com/sanbei101/mini-minio/internal/storage"
)

var metaBufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 1024))
	},
}

// hashOrder hashes input key to return a consistent hashed integer slice (1-based indices).
// This matches MinIO's distribution algorithm.
func hashOrder(key string, cardinality int) []int {
	if cardinality <= 0 {
		return nil
	}
	nums := make([]int, cardinality)
	keyCrc := crc32.ChecksumIEEE([]byte(key))
	start := int(keyCrc % uint32(cardinality))
	for i := 1; i <= cardinality; i++ {
		nums[i-1] = 1 + ((start + i) % cardinality)
	}
	return nums
}

// shuffleDisks reorders disks based on the distribution slice.
func shuffleDisks[T any](disks []T, distribution []int) []T {
	if len(distribution) != len(disks) {
		return disks
	}
	shuffled := make([]T, len(disks))
	for i, blockIndex := range distribution {
		shuffled[blockIndex-1] = disks[i]
	}
	return shuffled
}

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
	Distribution []int             `json:"distribution,omitempty"`
}

const (
	objectLockStripes = 4096
	bucketLockStripes = 256
)

// erasureObjects implements ObjectLayer using erasure coding across multiple disks.
type erasureObjects struct {
	disks         []storage.API
	dataBlocks    int
	parityBlocks  int
	pool          *bpool.BytePoolCap
	hashSeed      maphash.Seed
	bucketLocks   [bucketLockStripes]*contextMutex
	objectLocks   [objectLockStripes]*contextMutex
	erasureEngine erasure.Erasure
}

func newErasureObjects(
	disks []storage.API,
	dataBlocks, parityBlocks int,
	pool *bpool.BytePoolCap,
) (*erasureObjects, error) {
	engine, err := erasure.New(dataBlocks, parityBlocks, pool)
	if err != nil {
		return nil, err
	}

	eo := &erasureObjects{
		disks:         disks,
		dataBlocks:    dataBlocks,
		parityBlocks:  parityBlocks,
		pool:          pool,
		hashSeed:      maphash.MakeSeed(),
		erasureEngine: engine,
	}
	for i := range eo.bucketLocks {
		eo.bucketLocks[i] = newContextMutex()
	}
	for i := range eo.objectLocks {
		eo.objectLocks[i] = newContextMutex()
	}
	return eo, nil
}

func (e *erasureObjects) statBucket(ctx context.Context, bucket string) (os.FileInfo, error) {
	var firstErr error
	for _, disk := range e.disks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		info, err := disk.StatBucket(ctx, bucket)
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

// mergeBucketInfos merges per-disk bucket lists by name, keeping the earliest
// Created time, and returns them sorted by name.
func mergeBucketInfos(perDisk [][]BucketInfo) []BucketInfo {
	bucketByName := map[string]BucketInfo{}
	for _, infos := range perDisk {
		for _, b := range infos {
			existing, exists := bucketByName[b.Name]
			if !exists || b.Created.Before(existing.Created) {
				bucketByName[b.Name] = b
			}
		}
	}
	buckets := make([]BucketInfo, 0, len(bucketByName))
	for _, b := range bucketByName {
		buckets = append(buckets, b)
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})
	return buckets
}

func (e *erasureObjects) listBucketInfos(ctx context.Context) ([]BucketInfo, error) {
	type result struct {
		infos []BucketInfo
		err   error
	}
	results := make([]result, len(e.disks))
	var wg sync.WaitGroup
	for i, disk := range e.disks {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		wg.Add(1)
		go func(idx int, d storage.API) {
			defer wg.Done()
			infos, err := d.ListBuckets(ctx)
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

	perDisk := make([][]BucketInfo, 0, len(results))
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
		perDisk = append(perDisk, r.infos)
	}
	if okDisks == 0 && firstErr != nil {
		return nil, firstErr
	}
	return mergeBucketInfos(perDisk), nil
}

func (e *erasureObjects) MakeBucket(ctx context.Context, bucket string) error {
	lock := e.bucketLock(bucket)
	if err := lock.Lock(ctx); err != nil {
		return err
	}
	defer lock.Unlock()
	errs := make([]error, len(e.disks))
	var wg sync.WaitGroup
	for i, d := range e.disks {
		wg.Add(1)
		go func(idx int, disk storage.API) {
			defer wg.Done()
			err := disk.MakeBucket(ctx, bucket)
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
	fi, err := e.statBucket(ctx, bucket)
	if errors.Is(err, storage.ErrNotFound) {
		return BucketInfo{}, ErrBucketNotFound
	}
	if err != nil {
		return BucketInfo{}, err
	}
	return BucketInfo{Name: bucket, Created: fi.ModTime()}, nil
}

func (e *erasureObjects) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	return e.listBucketInfos(ctx)
}

func (e *erasureObjects) DeleteBucket(ctx context.Context, bucket string) error {
	lock := e.bucketLock(bucket)
	if err := lock.Lock(ctx); err != nil {
		return err
	}
	defer lock.Unlock()
	var wg sync.WaitGroup
	errs := make([]error, len(e.disks))
	for i, d := range e.disks {
		wg.Add(1)
		go func(idx int, disk storage.API) {
			defer wg.Done()
			err := disk.DeleteBucket(ctx, bucket)
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
	lock := e.objectLock(bucket, object)
	if err := lock.Lock(ctx); err != nil {
		return ObjectInfo{}, err
	}
	defer lock.Unlock()

	type metaResult struct {
		meta *xlMeta
		err  error
	}
	oldMetaCh := make(chan metaResult, 1)
	go func() {
		m, err := e.readMeta(ctx, bucket, object)
		oldMetaCh <- metaResult{meta: m, err: err}
	}()

	dataDir := uuid.New().String()
	n, err := e.writePart(ctx, bucket, object, dataDir, 1, data)
	if err != nil {
		e.cleanupObjectData(ctx, bucket, object, dataDir, nil)
		return ObjectInfo{}, err
	}

	res := <-oldMetaCh
	if res.err != nil && !errors.Is(res.err, ErrObjectNotFound) {
		e.cleanupObjectData(ctx, bucket, object, dataDir, nil)
		return ObjectInfo{}, res.err
	}
	oldMeta := res.meta

	etag := data.MD5()
	now := time.Now().UTC()
	distribution := hashOrder(object, len(e.disks))
	meta := xlMeta{
		Name:         object,
		Bucket:       bucket,
		Size:         n,
		ModTime:      now,
		ETag:         etag,
		ContentType:  "application/octet-stream",
		DataDir:      dataDir,
		DataBlocks:   e.dataBlocks,
		ParityBlocks: e.parityBlocks,
		BlockSize:    erasure.BlockSize,
		Parts:        []ObjectPartInfo{{Number: 1, Size: e.erasureEngine.ShardFileSize(n), ActualSize: n}},
		Distribution: distribution,
	}
	if err := e.commitMeta(ctx, bucket, object, oldMeta, &meta); err != nil {
		return ObjectInfo{}, err
	}

	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        n,
		ModTime:     now,
		ETag:        etag,
		ContentType: meta.ContentType,
	}, nil
}

func (e *erasureObjects) writePart(
	ctx context.Context,
	bucket, object, dataDir string,
	partNum int,
	data *PutObjReader,
) (int64, error) {
	enc := e.erasureEngine
	distribution := hashOrder(object, len(e.disks))
	orderedDisks := shuffleDisks(e.disks, distribution)
	writers := make([]io.Writer, len(orderedDisks))
	files := make([]storage.ShardWriter, len(orderedDisks))

	for i, d := range orderedDisks {
		f, ferr := d.CreateShardFile(ctx, bucket, object, dataDir, partNum)
		if ferr != nil {
			for j := range i {
				err := files[j].Close()
				if err != nil {
					log.Error().Err(err).Msg("failed to close shard file")
				}
			}
			return 0, ferr
		}
		files[i] = storage.NewBufferedShardWriter(ctx, f, 2)
		writers[i] = files[i]
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

	n, encErr := enc.Encode(ctx, data, writers, buffer, writeQuorum)
	for _, f := range files {
		if closeErr := f.Close(); encErr == nil && closeErr != nil {
			encErr = closeErr
		}
	}
	if encErr != nil {
		return 0, encErr
	}
	return n, nil
}

func (e *erasureObjects) commitMeta(ctx context.Context, bucket, object string, oldMeta, meta *xlMeta) error {
	writeQuorum := e.dataBlocks
	if e.dataBlocks == e.parityBlocks {
		writeQuorum++
	}

	// Write-then-rename: write tmp files in parallel, then rename atomically.
	tmpWritten := make([]bool, len(e.disks))
	var wg sync.WaitGroup
	metaErrs := make([]error, len(e.disks))
	for i, d := range e.disks {
		wg.Add(1)
		go func(idx int, disk storage.API) {
			defer wg.Done()
			m := *meta
			m.DiskIndex = idx
			buf, ok := metaBufferPool.Get().(*bytes.Buffer)
			if !ok {
				buf = bytes.NewBuffer(make([]byte, 0, 1024))
			}
			buf.Reset()
			defer metaBufferPool.Put(buf)
			if err := json.MarshalWrite(buf, &m); err != nil {
				metaErrs[idx] = err
				return
			}
			if err := disk.WriteMetaTmp(ctx, bucket, object, buf.Bytes()); err != nil {
				metaErrs[idx] = err
				return
			}
			tmpWritten[idx] = true
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
		e.cleanupObjectData(ctx, bucket, object, meta.DataDir, nil)
		return fmt.Errorf("metadata write quorum not met (%d/%d)", writeOK, writeQuorum)
	}

	// Rename tmp -> final in parallel.
	var renameWg sync.WaitGroup
	renameErrs := make([]error, len(e.disks))
	for i, d := range e.disks {
		if !tmpWritten[i] {
			continue
		}
		renameWg.Add(1)
		go func(idx int, disk storage.API) {
			defer renameWg.Done()
			renameErrs[idx] = disk.RenameMeta(ctx, bucket, object)
		}(i, d)
	}
	renameWg.Wait()
	renamed := make([]bool, len(e.disks))
	renameOK := 0
	for i, err := range renameErrs {
		if !tmpWritten[i] || err != nil {
			continue
		}
		renamed[i] = true
		renameOK++
	}
	if oldMeta != nil && oldMeta.DataDir != meta.DataDir {
		e.cleanupObjectData(ctx, bucket, object, oldMeta.DataDir, renamed)
	}

	uncommitted := make([]bool, len(e.disks))
	for i := range uncommitted {
		uncommitted[i] = !renamed[i]
	}
	e.cleanupObjectData(ctx, bucket, object, meta.DataDir, uncommitted)
	if renameOK < writeQuorum {
		return fmt.Errorf("metadata rename quorum not met (%d/%d)", renameOK, writeQuorum)
	}
	return nil
}

type contextMutex struct {
	token chan struct{}
}

func newContextMutex() *contextMutex {
	m := &contextMutex{token: make(chan struct{}, 1)}
	m.token <- struct{}{}
	return m
}

func (m *contextMutex) Lock(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.token:
		return nil
	}
}

func (m *contextMutex) Unlock() {
	select {
	case m.token <- struct{}{}:
	default:
		panic("unlock of unlocked contextMutex")
	}
}

type objectLockKey struct {
	bucket string
	object string
}

func (e *erasureObjects) objectLock(bucket, object string) *contextMutex {
	idx := maphash.Comparable(e.hashSeed, objectLockKey{bucket: bucket, object: object}) & (objectLockStripes - 1)
	return e.objectLocks[idx]
}

func (e *erasureObjects) bucketLock(bucket string) *contextMutex {
	idx := maphash.String(e.hashSeed, bucket) & (bucketLockStripes - 1)
	return e.bucketLocks[idx]
}

func (e *erasureObjects) cleanupObjectData(
	ctx context.Context,
	bucket, object, dataDir string,
	selected []bool,
) {
	var wg sync.WaitGroup
	for i, disk := range e.disks {
		if selected != nil && !selected[i] {
			continue
		}
		wg.Add(1)
		go func(d storage.API) {
			defer wg.Done()
			if err := d.DeleteObjectData(ctx, bucket, object, dataDir); err != nil {
				log.Warn().Err(err).Str("bucket", bucket).Str("object", object).Msg("object data cleanup failed")
			}
		}(disk)
	}
	wg.Wait()
}

func (e *erasureObjects) GetObjectInfo(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	meta, err := e.readMeta(ctx, bucket, object)
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
		Parts:       meta.Parts,
	}, nil
}

func (e *erasureObjects) GetObjectNInfo(
	ctx context.Context,
	bucket, object string,
	rs *HTTPRangeSpec,
) (*GetObjectReader, error) {
	meta, err := e.readMeta(ctx, bucket, object)
	if err != nil {
		return nil, err
	}

	enc := e.erasureEngine

	offset, length := int64(0), meta.Size
	if rs != nil {
		offset, length, err = rs.GetOffsetLength(meta.Size)
		if err != nil {
			return nil, err
		}
	}

	pr, pw := io.Pipe()
	go func() {
		decErr := e.decodeObject(ctx, pw, meta, offset, length, enc)
		pw.CloseWithError(decErr)
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

func (e *erasureObjects) decodeObject(
	ctx context.Context,
	dst io.Writer,
	meta *xlMeta,
	offset, length int64,
	enc erasure.Erasure,
) error {
	dist := meta.Distribution
	if len(dist) == 0 {
		dist = hashOrder(meta.Name, len(e.disks))
	}
	if len(meta.Parts) <= 1 {
		readers, closers := e.openShardReaders(ctx, meta.Bucket, meta.Name, meta.DataDir, 1, dist)
		decodeErr := enc.Decode(ctx, dst, readers, offset, length, meta.Size)
		return errors.Join(decodeErr, closeShardReaders(closers))
	}

	var objectOffset int64
	requestEnd := offset + length
	for _, part := range meta.Parts {
		partStart := objectOffset
		partEnd := partStart + part.ActualSize
		objectOffset = partEnd
		start := max(offset, partStart)
		end := min(requestEnd, partEnd)
		if start >= end {
			continue
		}

		readers, closers := e.openShardReaders(ctx, meta.Bucket, meta.Name, meta.DataDir, part.Number, dist)
		err := enc.Decode(ctx, dst, readers, start-partStart, end-start, part.ActualSize)
		closeErr := closeShardReaders(closers)
		if err = errors.Join(err, closeErr); err != nil {
			return err
		}
	}
	return nil
}

func (e *erasureObjects) openShardReaders(
	ctx context.Context,
	bucket, object, dataDir string,
	partNumber int,
	distribution []int,
) ([]io.ReaderAt, []io.Closer) {
	orderedDisks := shuffleDisks(e.disks, distribution)
	readers := make([]io.ReaderAt, len(orderedDisks))
	closers := make([]io.Closer, len(orderedDisks))
	for i, disk := range orderedDisks {
		reader, err := disk.ReadShardFile(ctx, bucket, object, dataDir, partNumber)
		if err == nil {
			readers[i] = reader
			closers[i] = reader
		}
	}
	return readers, closers
}

func closeShardReaders(closers []io.Closer) error {
	var closeErr error
	for _, closer := range closers {
		if closer != nil {
			closeErr = errors.Join(closeErr, closer.Close())
		}
	}
	return closeErr
}

func (e *erasureObjects) DeleteObject(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	lock := e.objectLock(bucket, object)
	if err := lock.Lock(ctx); err != nil {
		return ObjectInfo{}, err
	}
	defer lock.Unlock()

	info, err := e.GetObjectInfo(ctx, bucket, object)
	if err != nil {
		return ObjectInfo{}, err
	}

	errs := make([]error, len(e.disks))
	var wg sync.WaitGroup
	for i, d := range e.disks {
		wg.Add(1)
		go func(idx int, disk storage.API) {
			defer wg.Done()

			if ctx.Err() != nil {
				errs[idx] = ctx.Err()
				return
			}
			if disk == nil {
				errs[idx] = errors.New("disk not found")
				return
			}
			errs[idx] = disk.DeleteObject(ctx, bucket, object)
		}(i, d)
	}
	wg.Wait()

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
	writeQuorum := len(e.disks)/2 + 1
	successCount := len(e.disks) - failCount

	if successCount < writeQuorum {
		return ObjectInfo{}, fmt.Errorf("delete failed: only %d/%d disks succeeded", successCount, len(e.disks))
	}

	return info, nil
}

// readMeta reads xl.meta from disks. It first attempts to read and unmarshal xl.meta from
// the first available online disk; if reading or parsing fails, it falls back to full parallel quorum voting.
func (e *erasureObjects) readMeta(ctx context.Context, bucket, object string) (*xlMeta, error) {
	for _, d := range e.disks {
		if d == nil {
			continue
		}
		rc, err := d.ReadMeta(ctx, bucket, object)
		if err != nil {
			continue
		}
		var m xlMeta
		err = json.UnmarshalRead(rc, &m)
		rc.Close()
		if err == nil {
			return &m, nil
		}
	}

	metas := make([]*xlMeta, len(e.disks))
	errs := make([]error, len(e.disks))
	var wg sync.WaitGroup
	for i, d := range e.disks {
		wg.Add(1)
		go func(idx int, disk storage.API) {
			defer wg.Done()
			var m xlMeta
			rc, err := disk.ReadMeta(ctx, bucket, object)
			if err != nil {
				errs[idx] = err
				return
			}
			err = json.UnmarshalRead(rc, &m)
			rc.Close()
			if err != nil {
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

type multipartState struct {
	Bucket  string `json:"bucket"`
	Object  string `json:"object"`
	DataDir string `json:"dataDir"`
}

func (e *erasureObjects) NewMultipartUpload(ctx context.Context, bucket, object string) (string, error) {
	if _, err := e.GetBucketInfo(ctx, bucket); err != nil {
		return "", err
	}
	uploadID := uuid.New().String()
	state := multipartState{Bucket: bucket, Object: object, DataDir: uuid.New().String()}
	if err := e.writeUploadMeta(ctx, bucket, object, uploadID, "state", state); err != nil {
		return "", err
	}
	return uploadID, nil
}

func (e *erasureObjects) PutObjectPart(
	ctx context.Context,
	bucket, object, uploadID string,
	partNumber int,
	data *PutObjReader,
) (ObjectPartInfo, error) {
	if partNumber < 1 {
		return ObjectPartInfo{}, errors.New("invalid part number")
	}
	state, err := e.readMultipartState(ctx, bucket, object, uploadID)
	if err != nil {
		return ObjectPartInfo{}, err
	}
	n, err := e.writePart(ctx, bucket, object, state.DataDir, partNumber, data)
	if err != nil {
		return ObjectPartInfo{}, err
	}
	part := ObjectPartInfo{
		ETag:       data.MD5(),
		Number:     partNumber,
		Size:       e.erasureEngine.ShardFileSize(n),
		ActualSize: n,
		ModTime:    time.Now().UTC(),
	}
	if err := e.writeUploadMeta(ctx, bucket, object, uploadID, multipartPartName(partNumber), part); err != nil {
		return ObjectPartInfo{}, err
	}
	return part, nil
}

func (e *erasureObjects) CompleteMultipartUpload(
	ctx context.Context,
	bucket, object, uploadID string,
	partNumbers []int,
) (ObjectInfo, error) {
	lock := e.objectLock(bucket, object)
	if err := lock.Lock(ctx); err != nil {
		return ObjectInfo{}, err
	}
	defer lock.Unlock()

	state, err := e.readMultipartState(ctx, bucket, object, uploadID)
	if err != nil {
		return ObjectInfo{}, err
	}
	parts := make([]ObjectPartInfo, 0, len(partNumbers))
	seen := make(map[int]bool, len(partNumbers))
	var size int64
	for _, number := range partNumbers {
		if seen[number] {
			return ObjectInfo{}, fmt.Errorf("part %d selected more than once", number)
		}
		seen[number] = true
		part, err := e.readMultipartPart(ctx, bucket, object, uploadID, number)
		if err != nil {
			return ObjectInfo{}, err
		}
		parts = append(parts, part)
		size += part.ActualSize
	}
	if len(parts) == 0 {
		return ObjectInfo{}, errors.New("multipart upload has no parts")
	}

	oldMeta, err := e.readMeta(ctx, bucket, object)
	if err != nil && !errors.Is(err, ErrObjectNotFound) {
		return ObjectInfo{}, err
	}
	now := time.Now().UTC()
	etag := multipartETag(parts)
	distribution := hashOrder(object, len(e.disks))
	meta := xlMeta{
		Name:         object,
		Bucket:       bucket,
		Size:         size,
		ModTime:      now,
		ETag:         etag,
		ContentType:  "application/octet-stream",
		DataDir:      state.DataDir,
		DataBlocks:   e.dataBlocks,
		ParityBlocks: e.parityBlocks,
		BlockSize:    erasure.BlockSize,
		Parts:        parts,
		Distribution: distribution,
	}
	if err := e.commitMeta(ctx, bucket, object, oldMeta, &meta); err != nil {
		return ObjectInfo{}, err
	}
	if err := e.deleteUpload(ctx, bucket, object, uploadID); err != nil {
		return ObjectInfo{}, err
	}
	return ObjectInfo{
		Bucket:      bucket,
		Name:        object,
		Size:        size,
		ModTime:     now,
		ETag:        etag,
		ContentType: meta.ContentType,
		Parts:       parts,
	}, nil
}

func (e *erasureObjects) AbortMultipartUpload(ctx context.Context, bucket, object, uploadID string) error {
	state, err := e.readMultipartState(ctx, bucket, object, uploadID)
	if err != nil {
		return err
	}
	e.cleanupObjectData(ctx, bucket, object, state.DataDir, nil)
	return e.deleteUpload(ctx, bucket, object, uploadID)
}

func (e *erasureObjects) writeUploadMeta(
	ctx context.Context,
	bucket, object, uploadID, name string,
	value any,
) error {
	buf, ok := metaBufferPool.Get().(*bytes.Buffer)
	if !ok {
		buf = bytes.NewBuffer(make([]byte, 0, 1024))
	}
	buf.Reset()
	defer metaBufferPool.Put(buf)
	if err := json.MarshalWrite(buf, value); err != nil {
		return err
	}
	data := buf.Bytes()
	errs := make([]error, len(e.disks))
	var wg sync.WaitGroup
	for i, disk := range e.disks {
		wg.Add(1)
		go func(index int, drive storage.API) {
			defer wg.Done()
			errs[index] = drive.WriteUploadMeta(ctx, bucket, object, uploadID, name, data)
		}(i, disk)
	}
	wg.Wait()
	return e.checkWriteQuorum(errs, "upload metadata")
}

func (e *erasureObjects) readMultipartState(
	ctx context.Context,
	bucket, object, uploadID string,
) (multipartState, error) {
	var state multipartState
	if err := e.readUploadMeta(ctx, bucket, object, uploadID, "state", &state); err != nil {
		return multipartState{}, err
	}
	if state.Bucket != bucket || state.Object != object || state.DataDir == "" {
		return multipartState{}, errors.New("invalid multipart upload")
	}
	return state, nil
}

func (e *erasureObjects) readMultipartPart(
	ctx context.Context,
	bucket, object, uploadID string,
	partNumber int,
) (ObjectPartInfo, error) {
	var part ObjectPartInfo
	if err := e.readUploadMeta(ctx, bucket, object, uploadID, multipartPartName(partNumber), &part); err != nil {
		return ObjectPartInfo{}, err
	}
	if part.Number != partNumber {
		return ObjectPartInfo{}, errors.New("invalid multipart part")
	}
	return part, nil
}

func (e *erasureObjects) readUploadMeta(
	ctx context.Context,
	bucket, object, uploadID, name string,
	out any,
) error {
	for _, disk := range e.disks {
		if err := ctx.Err(); err != nil {
			return err
		}
		rc, err := disk.ReadUploadMeta(ctx, bucket, object, uploadID, name)
		if err == nil {
			err = json.UnmarshalRead(rc, out)
			_ = rc.Close()
			if err == nil {
				return nil
			}
		}
	}
	return errors.New("multipart upload not found")
}

func (e *erasureObjects) deleteUpload(ctx context.Context, bucket, object, uploadID string) error {
	errs := make([]error, len(e.disks))
	var wg sync.WaitGroup
	for i, disk := range e.disks {
		wg.Add(1)
		go func(index int, drive storage.API) {
			defer wg.Done()
			errs[index] = drive.DeleteUpload(ctx, bucket, object, uploadID)
		}(i, disk)
	}
	wg.Wait()
	return e.checkWriteQuorum(errs, "upload deletion")
}

func (e *erasureObjects) checkWriteQuorum(errs []error, operation string) error {
	quorum := e.dataBlocks
	if e.dataBlocks == e.parityBlocks {
		quorum++
	}
	ok := 0
	for _, err := range errs {
		if err == nil {
			ok++
		}
	}
	if ok < quorum {
		return fmt.Errorf("%s quorum not met (%d/%d)", operation, ok, quorum)
	}
	return nil
}

func multipartPartName(number int) string { return "part-" + strconv.Itoa(number) }

func multipartETag(parts []ObjectPartInfo) string {
	hash := md5.New()
	for _, part := range parts {
		decoded, err := hex.DecodeString(part.ETag)
		if err == nil {
			hash.Write(decoded)
		}
	}
	return fmt.Sprintf("%x-%d", hash.Sum(nil), len(parts))
}
