package cmd_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/sanbei101/mini-minio/cmd"
	"github.com/sanbei101/mini-minio/internal/erasure"
)

// createErasureLayer creates erasure objects with specified disk configuration.
// Uses tb.TempDir() which auto-cleans after test/benchmark completes.
func createErasureLayer(tb testing.TB, dataBlocks, parityBlocks int) cmd.ObjectLayer {
	tb.Helper()
	totalDisks := dataBlocks + parityBlocks
	disks := make([]string, totalDisks)
	for i := range disks {
		disks[i] = tb.TempDir()
	}
	obj, err := cmd.NewErasureObjects(disks, dataBlocks, parityBlocks)
	if err != nil {
		tb.Fatal(err)
	}
	return obj
}

// BenchmarkPutObject_SmallFile measures small file (1KB) write performance.
// Object names are reused to minimize disk usage during benchmark.
func BenchmarkPutObject_SmallFile(b *testing.B) {
	obj := createErasureLayer(b, 4, 2)
	ctx := context.Background()
	obj.MakeBucket(ctx, "bench-bucket")

	b.ReportAllocs()
	for b.Loop() {
		data := bytes.NewReader(make([]byte, 1024))
		reader, _ := cmd.NewPutObjReader(data, 1024)
		// Reuse same object name to avoid unbounded disk growth
		_, err := obj.PutObject(ctx, "bench-bucket", "small", reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutObject_MediumFile measures medium file (100KB) write performance.
func BenchmarkPutObject_MediumFile(b *testing.B) {
	obj := createErasureLayer(b, 4, 2)
	ctx := context.Background()
	obj.MakeBucket(ctx, "bench-bucket")

	b.ReportAllocs()
	for b.Loop() {
		data := bytes.NewReader(make([]byte, 100*1024))
		reader, _ := cmd.NewPutObjReader(data, 100*1024)
		_, err := obj.PutObject(ctx, "bench-bucket", "medium", reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPutObject_LargeFile measures large file (1MB) write performance.
func BenchmarkPutObject_LargeFile(b *testing.B) {
	obj := createErasureLayer(b, 4, 2)
	ctx := context.Background()
	obj.MakeBucket(ctx, "bench-bucket")

	b.ReportAllocs()
	for b.Loop() {
		data := bytes.NewReader(make([]byte, 1024*1024))
		reader, _ := cmd.NewPutObjReader(data, 1024*1024)
		_, err := obj.PutObject(ctx, "bench-bucket", "large", reader)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetObject measures read performance.
func BenchmarkGetObject(b *testing.B) {
	obj := createErasureLayer(b, 4, 2)
	ctx := context.Background()
	obj.MakeBucket(ctx, "bench-bucket")

	// Pre-populate one object only
	data := bytes.NewReader(make([]byte, 100*1024))
	reader, _ := cmd.NewPutObjReader(data, 100*1024)
	_, err := obj.PutObject(ctx, "bench-bucket", "test-object", reader)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		r, err := obj.GetObjectNInfo(ctx, "bench-bucket", "test-object", nil)
		if err != nil {
			b.Fatal(err)
		}
		io.ReadAll(r.Reader)
		r.Close()
	}
}

// BenchmarkPutObject_Parallel measures parallel write performance.
// Uses fixed object names to control disk usage.
func BenchmarkPutObject_Parallel(b *testing.B) {
	b.Run("4+2 disks", func(b *testing.B) {
		benchmarkPutObjectParallel(b, 4, 2)
	})
}

// Use small 20KB objects to limit disk usage during parallel test
func benchmarkPutObjectParallel(b *testing.B, dataBlocks, parityBlocks int) {
	obj := createErasureLayer(b, dataBlocks, parityBlocks)
	ctx := context.Background()
	obj.MakeBucket(ctx, "bench-bucket")

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			data := bytes.NewReader(make([]byte, 20*1024))
			reader, _ := cmd.NewPutObjReader(data, 20*1024)
			// Use counter mod 10 to reuse objects, limiting disk growth
			_, err := obj.PutObject(ctx, "bench-bucket", "parallel-"+itoa(counter%10), reader)
			if err != nil {
				b.Fatal(err)
			}
			counter++
		}
	})
}

// BenchmarkGetObject_Parallel measures parallel read performance.
// Pre-populates 10 objects only (20KB each) to save disk space.
func BenchmarkGetObject_Parallel(b *testing.B) {
	b.Run("4+2 disks", func(b *testing.B) {
		benchmarkGetObjectParallel(b, 4, 2)
	})
}

func benchmarkGetObjectParallel(b *testing.B, dataBlocks, parityBlocks int) {
	obj := createErasureLayer(b, dataBlocks, parityBlocks)
	ctx := context.Background()
	obj.MakeBucket(ctx, "bench-bucket")

	// Pre-populate only 10 objects (20KB each = ~120KB total across 6 disks)
	for i := range 10 {
		data := bytes.NewReader(make([]byte, 20*1024))
		reader, _ := cmd.NewPutObjReader(data, 20*1024)
		_, err := obj.PutObject(ctx, "bench-bucket", "test-"+itoa(i), reader)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			r, err := obj.GetObjectNInfo(ctx, "bench-bucket", "test-"+itoa(counter%10), nil)
			if err != nil {
				b.Fatal(err)
			}
			io.ReadAll(r.Reader)
			r.Close()
			counter++
		}
	})
}

// BenchmarkErasureEncode measures raw erasure encoding performance.
// Pure CPU benchmark - no disk I/O involved.
func BenchmarkErasureEncode(b *testing.B) {
	b.Run("4+2", func(b *testing.B) { benchmarkErasureEncode(b, 4, 2) })
	b.Run("6+2", func(b *testing.B) { benchmarkErasureEncode(b, 6, 2) })
}

func benchmarkErasureEncode(b *testing.B, dataBlocks, parityBlocks int) {
	enc, err := erasure.New(dataBlocks, parityBlocks, nil)
	if err != nil {
		b.Fatal(err)
	}

	// Use 1MB data - balances accuracy and speed
	data := make([]byte, 1024*1024)
	for i := range data {
		data[i] = byte(i % 256)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		_, err := enc.EncodeData(data)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkErasureDecode measures raw erasure decoding performance.
func BenchmarkErasureDecode(b *testing.B) {
	b.Run("4+2", func(b *testing.B) { benchmarkErasureDecode(b, 4, 2) })
}

func benchmarkErasureDecode(b *testing.B, dataBlocks, parityBlocks int) {
	enc, err := erasure.New(dataBlocks, parityBlocks, nil)
	if err != nil {
		b.Fatal(err)
	}

	data := make([]byte, 1024*1024)
	shards, err := enc.EncodeData(data)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-allocate decode buffer
	decodeBuf := make([][]byte, len(shards))
	for i := range shards {
		decodeBuf[i] = make([]byte, len(shards[i]))
	}

	b.ResetTimer()
	b.ReportAllocs()

	for b.Loop() {
		// Copy fresh data each iteration
		for i := range shards {
			copy(decodeBuf[i], shards[i])
		}
		err := enc.DecodeDataBlocks(decodeBuf)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkListObjects measures listing performance with limited objects.
func BenchmarkListObjects(b *testing.B) {
	obj := createErasureLayer(b, 4, 2)
	ctx := context.Background()
	obj.MakeBucket(ctx, "bench-bucket")

	// Pre-populate 100 small objects only
	for i := range 100 {
		data := bytes.NewReader(make([]byte, 512))
		reader, _ := cmd.NewPutObjReader(data, 512)
		_, err := obj.PutObject(ctx, "bench-bucket", "obj-"+itoa(i), reader)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		_, err := obj.ListObjectsV2(ctx, "bench-bucket", "", "", "", 100, "")
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDiskWriteComparison compares single disk vs multi-disk write.
// Uses small 50KB objects to limit disk usage.
func BenchmarkDiskWriteComparison(b *testing.B) {
	ctx := context.Background()

	b.Run("Single disk (1+0)", func(b *testing.B) {
		obj := createErasureLayer(b, 1, 0)
		obj.MakeBucket(ctx, "bench-bucket")
		b.ResetTimer()
		for b.Loop() {
			data := bytes.NewReader(make([]byte, 50*1024))
			reader, _ := cmd.NewPutObjReader(data, 50*1024)
			obj.PutObject(ctx, "bench-bucket", "single", reader)
		}
	})

	b.Run("4+2 disks", func(b *testing.B) {
		obj := createErasureLayer(b, 4, 2)
		obj.MakeBucket(ctx, "bench-bucket")
		b.ResetTimer()
		for b.Loop() {
			data := bytes.NewReader(make([]byte, 50*1024))
			reader, _ := cmd.NewPutObjReader(data, 50*1024)
			obj.PutObject(ctx, "bench-bucket", "multi42", reader)
		}
	})

	b.Run("6+2 disks", func(b *testing.B) {
		obj := createErasureLayer(b, 6, 2)
		obj.MakeBucket(ctx, "bench-bucket")
		b.ResetTimer()
		for b.Loop() {
			data := bytes.NewReader(make([]byte, 50*1024))
			reader, _ := cmd.NewPutObjReader(data, 50*1024)
			obj.PutObject(ctx, "bench-bucket", "multi62", reader)
		}
	})
}

// Helper function for int to string conversion
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
