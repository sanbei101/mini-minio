package cmd_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/sanbei101/mini-minio/cmd"
)

func TestClusterWritesAndReadsRemoteShards(t *testing.T) {
	listenerA := clusterListener(t)
	listenerB := clusterListener(t)
	nodeAURL := "http://" + listenerA.Addr().String()
	nodeBURL := "http://" + listenerB.Addr().String()
	drivesA := []string{t.TempDir(), t.TempDir()}
	drivesB := []string{t.TempDir(), t.TempDir()}
	endpoints := []string{
		nodeAURL + drivesA[0],
		nodeAURL + drivesA[1],
		nodeBURL + drivesB[0],
		nodeBURL + drivesB[1],
	}

	config := func(nodeURL string) cmd.ClusterConfig {
		return cmd.ClusterConfig{
			NodeURL:       nodeURL,
			Endpoints:     endpoints,
			DataBlocks:    2,
			ParityBlocks:  2,
			ClusterSecret: "test-cluster-secret",
		}
	}
	objectA, storageA, err := cmd.NewCluster(config(nodeAURL))
	if err != nil {
		t.Fatal(err)
	}
	objectB, storageB, err := cmd.NewCluster(config(nodeBURL))
	if err != nil {
		t.Fatal(err)
	}
	serveClusterHandler(t, listenerA, storageA)
	serveClusterHandler(t, listenerB, storageB)

	ctx := context.Background()
	if err := objectA.MakeBucket(ctx, "bucket"); err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("distributed-erasure"), 700000)
	reader, err := cmd.NewPutObjReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = objectA.PutObject(ctx, "bucket", "object", reader); err != nil {
		t.Fatal(err)
	}

	readerAtB, err := objectB.GetObjectNInfo(ctx, "bucket", "object", nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(readerAtB)
	if err := readerAtB.Close(); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("remote read body mismatch: got %d bytes, want %d", len(got), len(body))
	}

	for _, drive := range drivesB {
		matches, err := filepath.Glob(filepath.Join(drive, "bucket", "object", "*", "part.1"))
		if err != nil {
			t.Fatal(err)
		}
		if len(matches) != 1 {
			t.Fatalf("expected remote shard on %s, found %v", drive, matches)
		}
	}
}

func TestClusterMultipartPartsUseBothNodes(t *testing.T) {
	listenerA := clusterListener(t)
	listenerB := clusterListener(t)
	nodeAURL := "http://" + listenerA.Addr().String()
	nodeBURL := "http://" + listenerB.Addr().String()
	endpoints := []string{
		nodeAURL + t.TempDir(),
		nodeAURL + t.TempDir(),
		nodeBURL + t.TempDir(),
		nodeBURL + t.TempDir(),
	}
	config := func(nodeURL string) cmd.ClusterConfig {
		return cmd.ClusterConfig{
			NodeURL:       nodeURL,
			Endpoints:     endpoints,
			DataBlocks:    2,
			ParityBlocks:  2,
			ClusterSecret: "test-cluster-secret",
		}
	}
	objectA, storageA, err := cmd.NewCluster(config(nodeAURL))
	if err != nil {
		t.Fatal(err)
	}
	objectB, storageB, err := cmd.NewCluster(config(nodeBURL))
	if err != nil {
		t.Fatal(err)
	}
	serveClusterHandler(t, listenerA, storageA)
	serveClusterHandler(t, listenerB, storageB)

	ctx := context.Background()
	if err := objectA.MakeBucket(ctx, "bucket"); err != nil {
		t.Fatal(err)
	}
	uploadID, err := objectA.NewMultipartUpload(ctx, "bucket", "multipart")
	if err != nil {
		t.Fatal(err)
	}
	parts := [][]byte{
		bytes.Repeat([]byte("part-one"), 100000),
		bytes.Repeat([]byte("part-two"), 100000),
	}
	for i, part := range parts {
		reader, err := cmd.NewPutObjReader(bytes.NewReader(part), int64(len(part)))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = objectB.PutObjectPart(ctx, "bucket", "multipart", uploadID, i+1, reader); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = objectA.CompleteMultipartUpload(ctx, "bucket", "multipart", uploadID, []int{1, 2}); err != nil {
		t.Fatal(err)
	}

	reader, err := objectB.GetObjectNInfo(ctx, "bucket", "multipart", nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	want := append(append([]byte(nil), parts[0]...), parts[1]...)
	if !bytes.Equal(got, want) {
		t.Fatalf("multipart body mismatch: got %d bytes, want %d", len(got), len(want))
	}
}

func clusterListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func serveClusterHandler(t *testing.T, listener net.Listener, handler http.Handler) {
	t.Helper()
	server := &http.Server{Handler: handler}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			panic(fmt.Sprintf("cluster test server: %v", err))
		}
	}()
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Error(err)
		}
	})
}

func TestClusterStorageRequestRejectsWrongSecret(t *testing.T) {
	listener := clusterListener(t)
	drivePath := t.TempDir()
	nodeURL := "http://" + listener.Addr().String()
	_, storageHandler, err := cmd.NewCluster(cmd.ClusterConfig{
		NodeURL:       nodeURL,
		Endpoints:     []string{nodeURL + drivePath, "http://127.0.0.1:65535" + t.TempDir()},
		DataBlocks:    1,
		ParityBlocks:  1,
		ClusterSecret: "correct-secret",
	})
	if err != nil {
		t.Fatal(err)
	}
	serveClusterHandler(t, listener, storageHandler)

	response, err := http.Get(nodeURL + cmd.StorageRESTPrefix() + "0/buckets")
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %d", response.StatusCode)
	}
	if _, err = io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	if _, err = os.Stat(drivePath); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}
