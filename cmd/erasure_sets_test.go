package cmd_test

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/sanbei101/mini-minio/cmd"
)

func TestErasureSetsHashingIsStable(t *testing.T) {
	disks := make([]string, 12)
	for i := range disks {
		disks[i] = t.TempDir()
	}

	obj, err := cmd.NewErasureSets(disks, 4, 2)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := obj.MakeBucket(ctx, "bucket"); err != nil {
		t.Fatal(err)
	}

	keys := []string{
		"alpha.txt",
		"nested/a.txt",
		"nested/b.txt",
		"z-last.bin",
	}
	for _, key := range keys {
		reader, _ := cmd.NewPutObjReader(bytes.NewReader([]byte(key)), int64(len(key)))
		if _, err := obj.PutObject(ctx, "bucket", key, reader); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	for _, key := range keys {
		r, err := obj.GetObjectNInfo(ctx, "bucket", key, nil, nil)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		body, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatalf("read %s: %v", key, err)
		}
		if got := string(body); got != key {
			t.Fatalf("object %s routed inconsistently, got %q", key, got)
		}
	}
}

func TestErasureSetsListObjectsAcrossSets(t *testing.T) {
	disks := make([]string, 12)
	for i := range disks {
		disks[i] = t.TempDir()
	}

	obj, err := cmd.NewErasureSets(disks, 4, 2)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	if err := obj.MakeBucket(ctx, "bucket"); err != nil {
		t.Fatal(err)
	}

	names := []string{
		"dir/a.txt",
		"dir/b.txt",
		"other/c.txt",
		"root.txt",
	}
	for _, name := range names {
		reader, _ := cmd.NewPutObjReader(bytes.NewReader([]byte(name)), int64(len(name)))
		if _, err := obj.PutObject(ctx, "bucket", name, reader); err != nil {
			t.Fatalf("put %s: %v", name, err)
		}
	}

	list, err := obj.ListObjectsV2(ctx, "bucket", "", "", "/", 1000, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Objects) != 1 || list.Objects[0].Name != "root.txt" {
		t.Fatalf("unexpected root objects: %+v", list.Objects)
	}
	if len(list.Prefixes) != 2 || list.Prefixes[0] != "dir/" || list.Prefixes[1] != "other/" {
		t.Fatalf("unexpected prefixes: %+v", list.Prefixes)
	}

	list, err = obj.ListObjectsV2(ctx, "bucket", "dir/", "", "", 1000, false, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Objects) != 2 || list.Objects[0].Name != "dir/a.txt" || list.Objects[1].Name != "dir/b.txt" {
		t.Fatalf("unexpected dir listing: %+v", list.Objects)
	}
}

func TestErasureSetsRejectInvalidDiskLayout(t *testing.T) {
	disks := []string{t.TempDir(), t.TempDir(), t.TempDir()}
	if _, err := cmd.NewErasureSets(disks, 2, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := cmd.NewErasureSets(disks[:2], 2, 1); err == nil {
		t.Fatal("expected invalid argument error")
	}
}
