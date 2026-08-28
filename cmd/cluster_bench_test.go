package cmd_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/phuslu/log"

	"github.com/sanbei101/mini-minio/cmd"
)

func BenchmarkClusterPut(b *testing.B) {
	objectA, _, bucket := setupClusterBenchmark(b)
	payload := bytes.Repeat([]byte("cluster-put"), 100000)
	var objectID atomic.Uint64

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		name := "put-" + strconv.FormatUint(objectID.Add(1), 10)
		reader, err := cmd.NewPutObjReader(bytes.NewReader(payload), int64(len(payload)))
		if err != nil {
			b.Fatal(err)
		}
		if _, err = objectA.PutObject(context.Background(), bucket, name, reader); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkClusterPutParallel(b *testing.B) {
	objectA, objectB, bucket := setupClusterBenchmark(b)
	payload := bytes.Repeat([]byte("cluster-put-parallel"), 100000)
	var objectID atomic.Uint64

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := objectID.Add(1)
			objectLayer := objectA
			if id%2 == 0 {
				objectLayer = objectB
			}
			name := "parallel-" + strconv.FormatUint(id, 10)
			reader, err := cmd.NewPutObjReader(bytes.NewReader(payload), int64(len(payload)))
			if err != nil {
				b.Fatal(err)
			}
			if _, err = objectLayer.PutObject(context.Background(), bucket, name, reader); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkClusterGetParallel(b *testing.B) {
	objectA, objectB, bucket := setupClusterBenchmark(b)
	payload := bytes.Repeat([]byte("cluster-get"), 100000)
	const objectCount = 8

	for i := range objectCount {
		reader, err := cmd.NewPutObjReader(bytes.NewReader(payload), int64(len(payload)))
		if err != nil {
			b.Fatal(err)
		}
		if _, err = objectA.PutObject(context.Background(), bucket, "get-"+strconv.Itoa(i), reader); err != nil {
			b.Fatal(err)
		}
	}

	var objectID atomic.Uint64
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := objectID.Add(1)
			objectLayer := objectA
			if id%2 == 0 {
				objectLayer = objectB
			}
			name := "get-" + strconv.FormatUint(id%objectCount, 10)
			reader, err := objectLayer.GetObjectNInfo(context.Background(), bucket, name, nil)
			if err != nil {
				b.Fatal(err)
			}
			_, err = io.Copy(io.Discard, reader)
			closeErr := reader.Close()
			if err == nil {
				err = closeErr
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkClusterMultipartParallel(b *testing.B) {
	objectA, objectB, bucket := setupClusterBenchmark(b)
	part := bytes.Repeat([]byte("cluster-multipart"), 400000)
	var objectID atomic.Uint64

	b.SetBytes(int64(2 * len(part)))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := objectID.Add(1)
			name := "multipart-" + strconv.FormatUint(id, 10)
			uploadID, err := objectA.NewMultipartUpload(context.Background(), bucket, name)
			if err != nil {
				b.Fatal(err)
			}

			first, err := cmd.NewPutObjReader(bytes.NewReader(part), int64(len(part)))
			if err != nil {
				b.Fatal(err)
			}
			if _, err = objectA.PutObjectPart(context.Background(), bucket, name, uploadID, 1, first); err != nil {
				b.Fatal(err)
			}

			second, err := cmd.NewPutObjReader(bytes.NewReader(part), int64(len(part)))
			if err != nil {
				b.Fatal(err)
			}
			if _, err = objectB.PutObjectPart(context.Background(), bucket, name, uploadID, 2, second); err != nil {
				b.Fatal(err)
			}
			if _, err = objectA.CompleteMultipartUpload(
				context.Background(),
				bucket,
				name,
				uploadID,
				[]int{1, 2},
			); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func setupClusterBenchmark(b *testing.B) (cmd.ObjectLayer, cmd.ObjectLayer, string) {
	b.Helper()
	listenerA, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	listenerB, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if closeErr := listenerA.Close(); closeErr != nil {
			b.Error(closeErr)
		}
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := listenerA.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Error().Err(err).Msg("failed to close listenerA")
		}
		if err := listenerB.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Error().Err(err).Msg("failed to close listenerB")
		}
	})
	nodeAURL := "http://" + listenerA.Addr().String()
	nodeBURL := "http://" + listenerB.Addr().String()
	endpoints := []string{
		nodeAURL + b.TempDir(),
		nodeAURL + b.TempDir(),
		nodeBURL + b.TempDir(),
		nodeBURL + b.TempDir(),
	}
	config := func(nodeURL string) cmd.ClusterConfig {
		return cmd.ClusterConfig{
			NodeURL:       nodeURL,
			Endpoints:     endpoints,
			DataBlocks:    2,
			ParityBlocks:  2,
			ClusterSecret: "benchmark-cluster-secret",
		}
	}
	objectA, storageA, err := cmd.NewCluster(config(nodeAURL))
	if err != nil {
		b.Fatal(err)
	}
	objectB, storageB, err := cmd.NewCluster(config(nodeBURL))
	if err != nil {
		b.Fatal(err)
	}

	serverA := &http.Server{Handler: storageA}
	serverB := &http.Server{Handler: storageB}
	go func() {
		if err := serverA.Serve(listenerA); err != nil && !errors.Is(err, http.ErrServerClosed) {
			b.Error(err)
		}
	}()
	go func() {
		if err := serverB.Serve(listenerB); err != nil && !errors.Is(err, http.ErrServerClosed) {
			b.Error(err)
		}
	}()
	b.Cleanup(func() {
		if err := serverA.Close(); err != nil {
			b.Error(err)
		}
		if err := serverB.Close(); err != nil {
			b.Error(err)
		}
	})
	const bucket = "cluster-benchmark"
	if err := objectA.MakeBucket(context.Background(), bucket); err != nil {
		b.Fatal(err)
	}
	return objectA, objectB, bucket
}
