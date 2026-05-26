package cmd_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/sanbei101/mini-minio/cmd"
)

func TestErasureSetsRouteAndListAcrossSets(t *testing.T) {
	ctx := context.Background()
	disks := make([]string, 12)
	for i := range disks {
		disks[i] = t.TempDir()
	}

	obj, err := cmd.NewErasureObjects(disks, 4, 2)
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

	result, err := obj.ListObjectsV2(ctx, "bucket", "", "", "", 100, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Objects) != len(objects) {
		t.Fatalf("want %d objects, got %d", len(objects), len(result.Objects))
	}

	reader, err := obj.GetObjectNInfo(ctx, "bucket", "object-000", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != objects["object-000"] {
		t.Fatalf("body mismatch: %q", body)
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
