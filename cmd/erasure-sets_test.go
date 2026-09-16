package cmd_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/sanbei101/mini-minio/cmd"
)

func TestErasureSetsRouteAndListAcrossSets(t *testing.T) {
	ctx := context.Background()
	disks := make([]string, 12)
	for i := range disks {
		disks[i] = t.TempDir()
	}

	obj, err := cmd.NewErasureSets(disks, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.MakeBucket(ctx, "bucket"); err != nil {
		t.Fatal(err)
	}

	objects := map[string]string{}
	for i := range 100 {
		name := fmt.Sprintf("object-%03d", i)
		body := "body-" + name
		reader, err := cmd.NewPutObjReader(bytes.NewReader([]byte(body)), int64(len(body)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = obj.PutObject(ctx, "bucket", name, reader); err != nil {
			t.Fatal(err)
		}
		objects[name] = body

		if countUsedSets(t, disks, "bucket") == 2 {
			break
		}
	}

	if countUsedSets(t, disks, "bucket") != 2 {
		t.Fatal("objects were not distributed to both sets")
	}

	result, err := obj.ListObjectsV2(ctx, "bucket", "", "", "", 100, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Objects) != len(objects) {
		t.Fatalf("want %d objects, got %d", len(objects), len(result.Objects))
	}

	reader, err := obj.GetObjectNInfo(ctx, "bucket", "object-000", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(reader)
	closeErr := reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	if string(body) != objects["object-000"] {
		t.Fatalf("body mismatch: %q", body)
	}
}

func TestErasureSetNamespaceDoesNotDependOnFirstDisk(t *testing.T) {
	ctx := context.Background()
	disks := make([]string, 6)
	for i := range disks {
		disks[i] = t.TempDir()
	}

	obj, err := cmd.NewErasureSets(disks, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.MakeBucket(ctx, "bucket"); err != nil {
		t.Fatal(err)
	}

	reader, err := cmd.NewPutObjReader(bytes.NewReader([]byte("data")), 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = obj.PutObject(ctx, "bucket", "object", reader); err != nil {
		t.Fatal(err)
	}

	if err := os.RemoveAll(filepath.Join(disks[0], "bucket")); err != nil {
		t.Fatal(err)
	}

	if _, err = obj.GetBucketInfo(ctx, "bucket"); err != nil {
		t.Fatal(err)
	}

	buckets, err := obj.ListBuckets(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 1 || buckets[0].Name != "bucket" {
		t.Fatalf("unexpected buckets: %#v", buckets)
	}

	result, err := obj.ListObjectsV2(ctx, "bucket", "", "", "", 100, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Objects) != 1 || result.Objects[0].Name != "object" {
		t.Fatalf("unexpected objects: %#v", result.Objects)
	}
}

func TestPutObjectOverwriteCleansOldData(t *testing.T) {
	ctx := context.Background()
	disks := make([]string, 6)
	for i := range disks {
		disks[i] = t.TempDir()
	}

	obj, err := cmd.NewErasureSets(disks, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.MakeBucket(ctx, "bucket"); err != nil {
		t.Fatal(err)
	}

	for _, body := range []string{"first", "second"} {
		reader, err := cmd.NewPutObjReader(bytes.NewReader([]byte(body)), int64(len(body)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = obj.PutObject(ctx, "bucket", "object", reader); err != nil {
			t.Fatal(err)
		}
	}

	reader, err := obj.GetObjectNInfo(ctx, "bucket", "object", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(reader)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "second" {
		t.Fatalf("want second body, got %q", body)
	}

	for _, disk := range disks {
		entries, err := os.ReadDir(filepath.Join(disk, "bucket", "object"))
		if err != nil {
			t.Fatal(err)
		}
		var dataDirs int
		for _, entry := range entries {
			if entry.IsDir() {
				dataDirs++
			}
		}
		if dataDirs != 1 {
			t.Fatalf("want one data directory on %s, got %d", disk, dataDirs)
		}
	}
}

func TestConcurrentPutObjectSameKey(t *testing.T) {
	ctx := context.Background()
	disks := make([]string, 6)
	for i := range disks {
		disks[i] = t.TempDir()
	}

	obj, err := cmd.NewErasureSets(disks, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.MakeBucket(ctx, "bucket"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf("body-%d", i)
			reader, err := cmd.NewPutObjReader(bytes.NewReader([]byte(body)), int64(len(body)))
			if err != nil {
				errs <- err
				return
			}
			_, err = obj.PutObject(ctx, "bucket", "object", reader)
			if err != nil {
				errs <- err
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	reader, err := obj.GetObjectNInfo(ctx, "bucket", "object", nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(reader)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(body), "body-") {
		t.Fatalf("unexpected body: %q", body)
	}

	for _, disk := range disks {
		entries, err := os.ReadDir(filepath.Join(disk, "bucket", "object"))
		if err != nil {
			t.Fatal(err)
		}
		var dataDirs int
		for _, entry := range entries {
			if entry.IsDir() {
				dataDirs++
			}
		}
		if dataDirs != 1 {
			t.Fatalf("want one data directory on %s, got %d", disk, dataDirs)
		}
	}
}

func countUsedSets(t *testing.T, disks []string, bucket string) int {
	t.Helper()
	var usedSets int
	for setIndex := range 2 {
		if setHasObjectMeta(t, disks[setIndex*6:(setIndex+1)*6], bucket) {
			usedSets++
		}
	}
	return usedSets
}

func setHasObjectMeta(t *testing.T, disks []string, bucket string) bool {
	t.Helper()
	for _, disk := range disks {
		entries, err := os.ReadDir(filepath.Join(disk, bucket))
		if err != nil {
			continue
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				continue
			}
			if _, err := os.Stat(filepath.Join(disk, bucket, entry.Name(), "xl.meta")); err == nil {
				return true
			}
		}
	}
	return false
}

func TestDiskShuffleRotation(t *testing.T) {
	ctx := context.Background()
	disks := make([]string, 4)
	for i := range disks {
		disks[i] = t.TempDir()
	}

	obj, err := cmd.NewErasureSets(disks, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if err := obj.MakeBucket(ctx, "shuffle-bucket"); err != nil {
		t.Fatal(err)
	}

	// Write 20 objects with different names and check that shard placement rotates.
	seenFirstDisks := make(map[int]bool)
	for i := range 20 {
		name := fmt.Sprintf("obj-%d", i)
		data := fmt.Sprintf("hello world %d", i)
		r, err := cmd.NewPutObjReader(bytes.NewReader([]byte(data)), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		info, err := obj.PutObject(ctx, "shuffle-bucket", name, r)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size != int64(len(data)) {
			t.Fatalf("expected size %d, got %d", len(data), info.Size)
		}

		// Read back object to verify integrity.
		reader, err := obj.GetObjectNInfo(ctx, "shuffle-bucket", name, nil)
		if err != nil {
			t.Fatal(err)
		}
		readBytes, err := io.ReadAll(reader)
		reader.Close()
		if err != nil {
			t.Fatal(err)
		}
		if string(readBytes) != data {
			t.Fatalf("expected %q, got %q", data, string(readBytes))
		}
	}

	// Verify that part_1 was written to different disks across the 20 objects.
	for _, disk := range disks {
		matches, _ := filepath.Glob(filepath.Join(disk, "shuffle-bucket", "obj-*", "*", "part.1"))
		if len(matches) > 0 {
			seenFirstDisks[len(seenFirstDisks)] = true
		}
	}
	if len(seenFirstDisks) < 2 {
		t.Fatalf("expected shards to be distributed to multiple disks, but only saw %d disks", len(seenFirstDisks))
	}
}
