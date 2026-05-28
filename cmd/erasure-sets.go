package cmd

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"net/http"
	"sort"
	"strings"

	"github.com/sanbei101/mini-minio/internal/bpool"
	"github.com/sanbei101/mini-minio/internal/erasure"
	"github.com/sanbei101/mini-minio/internal/storage"
)

// erasureSets is the ObjectLayer-facing collection of static erasure sets.
//
// MinIO's production implementation also owns disk reconnect, healing, locks,
// rebalance, metrics, and format migration. This mini version keeps only the
// core shape: split drives into fixed-size sets, route each object to one set,
// and merge namespace operations across all sets.
type erasureSets struct {
	sets          []*erasureObjects
	setDriveCount int
	dataBlocks    int
	parityBlocks  int
	bufferPool    *bpool.BytePoolCap
}

// NewErasureObjects creates an ObjectLayer backed by one or more erasure sets.
func NewErasureObjects(diskPaths []string, dataBlocks, parityBlocks int) (ObjectLayer, error) {
	setDriveCount := dataBlocks + parityBlocks
	if dataBlocks <= 0 || parityBlocks <= 0 {
		return nil, errors.New("data and parity blocks must be positive")
	}
	if len(diskPaths) == 0 || len(diskPaths)%setDriveCount != 0 {
		return nil, fmt.Errorf("need disk paths in groups of %d, got %d", setDriveCount, len(diskPaths))
	}

	// Create buffer pool: 1024 buffers of BlockSize, 4K-aligned.
	pool := bpool.NewBytePoolCap(1024, erasure.BlockSize, erasure.BlockSize)
	pool.Populate()

	setCount := len(diskPaths) / setDriveCount
	sets := make([]*erasureObjects, 0, setCount)
	for i := range setCount {
		start := i * setDriveCount
		set, err := newErasureObjects(diskPaths[start:start+setDriveCount], dataBlocks, parityBlocks, pool)
		if err != nil {
			return nil, err
		}
		sets = append(sets, set)
	}

	return &erasureSets{
		sets:          sets,
		setDriveCount: setDriveCount,
		dataBlocks:    dataBlocks,
		parityBlocks:  parityBlocks,
		bufferPool:    pool,
	}, nil
}

func (s *erasureSets) MakeBucket(ctx context.Context, bucket string) error {
	for _, set := range s.sets {
		if err := set.MakeBucket(ctx, bucket); err != nil {
			return err
		}
	}
	return nil
}

func (s *erasureSets) GetBucketInfo(ctx context.Context, bucket string) (BucketInfo, error) {
	var firstErr error
	for _, set := range s.sets {
		info, err := set.GetBucketInfo(ctx, bucket)
		if err == nil {
			return info, nil
		}
		if !errors.Is(err, ErrBucketNotFound) && firstErr == nil {
			firstErr = err
		}
	}
	if firstErr != nil {
		return BucketInfo{}, firstErr
	}
	return BucketInfo{}, ErrBucketNotFound
}

func (s *erasureSets) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	bucketByName := map[string]BucketInfo{}
	var firstErr error
	var okSets int

	for _, set := range s.sets {
		buckets, err := set.ListBuckets(ctx)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		okSets++
		for _, bucket := range buckets {
			existing, exists := bucketByName[bucket.Name]
			if !exists || bucket.Created.Before(existing.Created) {
				bucketByName[bucket.Name] = bucket
			}
		}
	}
	if okSets == 0 && firstErr != nil {
		return nil, firstErr
	}

	buckets := make([]BucketInfo, 0, len(bucketByName))
	for _, bucket := range bucketByName {
		buckets = append(buckets, bucket)
	}
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})
	return buckets, nil
}

func (s *erasureSets) DeleteBucket(ctx context.Context, bucket string) error {
	for _, set := range s.sets {
		if err := set.DeleteBucket(ctx, bucket); err != nil {
			return err
		}
	}
	return nil
}

func (s *erasureSets) ListObjectsV2(
	ctx context.Context,
	bucket, prefix, continuationToken, delimiter string,
	maxKeys int,
	fetchOwner bool,
	startAfter string,
) (ListObjectsV2Info, error) {
	_ = fetchOwner

	names, err := s.listObjectNames(bucket, prefix)
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

	result := ListObjectsV2Info{
		Objects:  []ObjectInfo{},
		Prefixes: []string{},
	}
	seenPrefix := map[string]bool{}

	for i, name := range names {
		if len(result.Objects)+len(result.Prefixes) >= maxKeys {
			result.IsTruncated = true
			result.NextContinuationToken = names[i-1]
			break
		}

		if delimiter != "" {
			rel := strings.TrimPrefix(name, prefix)
			if idx := strings.Index(rel, delimiter); idx >= 0 {
				commonPrefix := prefix + rel[:idx+len(delimiter)]
				if !seenPrefix[commonPrefix] {
					seenPrefix[commonPrefix] = true
					result.Prefixes = append(result.Prefixes, commonPrefix)
				}
				continue
			}
		}

		meta, err := s.setForObject(bucket, name).readMeta(bucket, name)
		if err != nil {
			continue
		}
		result.Objects = append(result.Objects, ObjectInfo{
			Bucket:      bucket,
			Name:        name,
			Size:        meta.Size,
			ModTime:     meta.ModTime,
			ETag:        meta.ETag,
			ContentType: meta.ContentType,
		})
	}

	return result, nil
}

func (s *erasureSets) GetObjectNInfo(
	ctx context.Context,
	bucket, object string,
	rs *HTTPRangeSpec,
	h http.Header,
) (*GetObjectReader, error) {
	return s.setForObject(bucket, object).GetObjectNInfo(ctx, bucket, object, rs, h)
}

func (s *erasureSets) GetObjectInfo(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	return s.setForObject(bucket, object).GetObjectInfo(ctx, bucket, object)
}

func (s *erasureSets) PutObject(ctx context.Context, bucket, object string, data *PutObjReader) (ObjectInfo, error) {
	return s.setForObject(bucket, object).PutObject(ctx, bucket, object, data)
}

func (s *erasureSets) DeleteObject(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	return s.setForObject(bucket, object).DeleteObject(ctx, bucket, object)
}

func (s *erasureSets) listObjectNames(bucket, prefix string) ([]string, error) {
	seen := map[string]bool{}
	names := []string{}
	var foundBucket bool

	for _, set := range s.sets {
		setNames, err := set.listObjectNames(bucket, prefix)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return nil, err
		}
		foundBucket = true
		for _, name := range setNames {
			if seen[name] {
				continue
			}
			seen[name] = true
			names = append(names, name)
		}
	}
	if !foundBucket {
		return nil, ErrBucketNotFound
	}

	sort.Strings(names)
	return names, nil
}

func (s *erasureSets) setForObject(bucket, object string) *erasureObjects {
	index := int(crc32.ChecksumIEEE([]byte(object)) % uint32(len(s.sets)))
	return s.sets[index]
}
