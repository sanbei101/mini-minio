package cmd

import (
	"context"
	"errors"
	"hash/crc32"
	"net/http"
	"sort"
)

// erasureSets is a minimal multi-set object layer.
// Bucket operations are applied to every set, and object operations are routed
// to one set based on a stable object-name hash.
type erasureSets struct {
	sets []*erasureObjects
}

func NewErasureSets(diskPaths []string, dataBlocks, parityBlocks int) (ObjectLayer, error) {
	disksPerSet := dataBlocks + parityBlocks
	if disksPerSet <= 0 {
		return nil, ErrInvalidArgument
	}
	if len(diskPaths) == 0 || len(diskPaths)%disksPerSet != 0 {
		return nil, ErrInvalidArgument
	}
	if len(diskPaths) == disksPerSet {
		return newSingleErasureObjects(diskPaths, dataBlocks, parityBlocks)
	}

	setCount := len(diskPaths) / disksPerSet
	sets := make([]*erasureObjects, 0, setCount)
	for i := 0; i < setCount; i++ {
		start := i * disksPerSet
		setObj, err := newSingleErasureObjects(diskPaths[start:start+disksPerSet], dataBlocks, parityBlocks)
		if err != nil {
			return nil, err
		}
		sets = append(sets, setObj)
	}
	return &erasureSets{sets: sets}, nil
}

func (s *erasureSets) getHashedSet(object string) *erasureObjects {
	if len(s.sets) == 1 {
		return s.sets[0]
	}
	idx := int(crc32.ChecksumIEEE([]byte(object)) % uint32(len(s.sets)))
	return s.sets[idx]
}

func (s *erasureSets) MakeBucket(ctx context.Context, bucket string) error {
	exists := 0
	for _, set := range s.sets {
		err := set.MakeBucket(ctx, bucket)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrBucketExists) {
			exists++
			continue
		}
		return err
	}
	if exists == len(s.sets) {
		return ErrBucketExists
	}
	return nil
}

func (s *erasureSets) GetBucketInfo(ctx context.Context, bucket string) (BucketInfo, error) {
	var lastErr error
	for _, set := range s.sets {
		info, err := set.GetBucketInfo(ctx, bucket)
		if err == nil {
			return info, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = ErrBucketNotFound
	}
	return BucketInfo{}, lastErr
}

func (s *erasureSets) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
	seen := make(map[string]BucketInfo)
	for _, set := range s.sets {
		buckets, err := set.ListBuckets(ctx)
		if err != nil {
			return nil, err
		}
		for _, bucket := range buckets {
			current, ok := seen[bucket.Name]
			if !ok || bucket.Created.Before(current.Created) {
				seen[bucket.Name] = bucket
			}
		}
	}

	out := make([]BucketInfo, 0, len(seen))
	for _, bucket := range seen {
		out = append(out, bucket)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func (s *erasureSets) DeleteBucket(ctx context.Context, bucket string) error {
	missing := 0
	for _, set := range s.sets {
		err := set.DeleteBucket(ctx, bucket)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrBucketNotFound) {
			missing++
			continue
		}
		return err
	}
	if missing == len(s.sets) {
		return ErrBucketNotFound
	}
	return nil
}

func (s *erasureSets) ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (ListObjectsV2Info, error) {
	var (
		allObjects []ObjectInfo
		missing    int
	)
	for _, set := range s.sets {
		objects, err := set.listObjectInfos(ctx, bucket, prefix)
		if err == nil {
			allObjects = append(allObjects, objects...)
			continue
		}
		if errors.Is(err, ErrBucketNotFound) {
			missing++
			continue
		}
		return ListObjectsV2Info{}, err
	}
	if missing == len(s.sets) {
		return ListObjectsV2Info{}, ErrBucketNotFound
	}

	sort.Slice(allObjects, func(i, j int) bool {
		return allObjects[i].Name < allObjects[j].Name
	})
	return buildListObjectsV2Result(allObjects, prefix, continuationToken, delimiter, maxKeys, startAfter), nil
}

func (s *erasureSets) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header) (*GetObjectReader, error) {
	return s.getHashedSet(object).GetObjectNInfo(ctx, bucket, object, rs, h)
}

func (s *erasureSets) GetObjectInfo(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	return s.getHashedSet(object).GetObjectInfo(ctx, bucket, object)
}

func (s *erasureSets) PutObject(ctx context.Context, bucket, object string, data *PutObjReader) (ObjectInfo, error) {
	return s.getHashedSet(object).PutObject(ctx, bucket, object, data)
}

func (s *erasureSets) DeleteObject(ctx context.Context, bucket, object string) (ObjectInfo, error) {
	return s.getHashedSet(object).DeleteObject(ctx, bucket, object)
}
