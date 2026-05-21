package cmd

import (
	"context"
	"net/http"
)

type ObjectLayer interface {
	MakeBucket(ctx context.Context, bucket string) error
	GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error)
	ListBuckets(ctx context.Context) (buckets []BucketInfo, err error)
	DeleteBucket(ctx context.Context, bucket string) error

	ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, fetchOwner bool, startAfter string) (result ListObjectsV2Info, err error)
	GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec, h http.Header) (reader *GetObjectReader, err error)
	GetObjectInfo(ctx context.Context, bucket, object string) (objInfo ObjectInfo, err error)
	PutObject(ctx context.Context, bucket, object string, data *PutObjReader) (objInfo ObjectInfo, err error)
	DeleteObject(ctx context.Context, bucket, object string) (ObjectInfo, error)
}
