package cmd

import (
	"bytes"
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sanbei101/mini-minio/internal/storage"
)

type storageRESTClient struct {
	baseURL    url.URL
	driveID    string
	secret     string
	configHash string
	client     *http.Client
}

func newStorageRESTClient(endpoint *url.URL, driveID, secret, configHash string) *storageRESTClient {
	return &storageRESTClient{
		baseURL:    url.URL{Scheme: endpoint.Scheme, Host: endpoint.Host},
		driveID:    driveID,
		secret:     secret,
		configHash: configHash,
		client:     http.DefaultClient,
	}
}

func (c *storageRESTClient) requestURL(operation string, values url.Values) string {
	u := c.baseURL
	u.Path = storageRESTPrefix + c.driveID + "/" + operation
	u.RawQuery = values.Encode()
	return u.String()
}

func (c *storageRESTClient) call(
	ctx context.Context,
	method, operation string,
	values url.Values,
	body io.Reader,
) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.requestURL(operation, values), body)
	if err != nil {
		return nil, err
	}
	timestamp := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set(storageRESTTimeHeader, timestamp)
	req.Header.Set(storageRESTConfigHeader, c.configHash)
	req.Header.Set(storageRESTAuthHeader, storageRESTSignature(
		c.secret,
		timestamp,
		method,
		req.URL.EscapedPath(),
		req.URL.RawQuery,
		c.configHash,
	))

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return resp, nil
	}
	message, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	closeErr := resp.Body.Close()
	responseErr := errors.Join(readErr, closeErr)
	switch resp.StatusCode {
	case http.StatusNotFound:
		return nil, errors.Join(storage.ErrNotFound, responseErr)
	case http.StatusConflict:
		return nil, errors.Join(storage.ErrBucketExists, responseErr)
	default:
		if responseErr != nil {
			return nil, fmt.Errorf("remote drive %s %s: %s: %w", operation, resp.Status, string(message), responseErr)
		}
		return nil, fmt.Errorf("remote drive %s %s: %s", operation, resp.Status, string(message))
	}
}

func (c *storageRESTClient) MakeBucket(bucket string) error {
	resp, err := c.call(context.Background(), http.MethodPut, "bucket", url.Values{"bucket": {bucket}}, nil)
	if resp != nil {
		err = errors.Join(err, resp.Body.Close())
	}
	return err
}

func (c *storageRESTClient) DeleteBucket(bucket string) error {
	resp, err := c.call(context.Background(), http.MethodDelete, "bucket", url.Values{"bucket": {bucket}}, nil)
	if resp != nil {
		err = errors.Join(err, resp.Body.Close())
	}
	return err
}

func (c *storageRESTClient) ListBuckets() ([]os.FileInfo, error) {
	resp, err := c.call(context.Background(), http.MethodGet, "buckets", nil, nil)
	if err != nil {
		return nil, err
	}
	var buckets []storageRESTFileInfo
	data, err := readStorageRESTBody(resp)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &buckets); err != nil {
		return nil, err
	}
	infos := make([]os.FileInfo, len(buckets))
	for i := range buckets {
		infos[i] = buckets[i]
	}
	return infos, nil
}

func (c *storageRESTClient) StatBucket(bucket string) (os.FileInfo, error) {
	resp, err := c.call(context.Background(), http.MethodGet, "bucket", url.Values{"bucket": {bucket}}, nil)
	if err != nil {
		return nil, err
	}
	var info storageRESTFileInfo
	data, err := readStorageRESTBody(resp)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return nil, err
	}
	return info, nil
}

func (c *storageRESTClient) CreateShardFile(
	ctx context.Context,
	bucket, object, dataDir string,
	partNum int,
) (storage.ShardWriter, error) {
	reader, writer := io.Pipe()
	done := make(chan error, 1)
	values := shardValues(bucket, object, dataDir, partNum)
	go func() {
		resp, err := c.call(ctx, http.MethodPost, "shard", values, reader)
		if resp != nil {
			_, drainErr := io.Copy(io.Discard, resp.Body)
			closeErr := resp.Body.Close()
			err = errors.Join(err, drainErr, closeErr)
		}
		if err != nil {
			err = errors.Join(err, reader.CloseWithError(err))
		} else {
			err = reader.Close()
		}
		done <- err
	}()
	return &storageRESTShardWriter{writer: writer, done: done}, nil
}

func (c *storageRESTClient) ReadShardFile(
	ctx context.Context,
	bucket, object, dataDir string,
	partNum int,
) (storage.ShardReader, error) {
	return &storageRESTShardReader{
		client:  c,
		ctx:     ctx,
		bucket:  bucket,
		object:  object,
		dataDir: dataDir,
		partNum: partNum,
	}, nil
}

func (c *storageRESTClient) DeleteObjectData(bucket, object, dataDir string) error {
	resp, err := c.call(context.Background(), http.MethodDelete, "data", shardValues(bucket, object, dataDir, 0), nil)
	if resp != nil {
		err = errors.Join(err, resp.Body.Close())
	}
	return err
}

func (c *storageRESTClient) WriteMetaTmp(bucket, object string, data []byte) error {
	resp, err := c.call(
		context.Background(),
		http.MethodPut,
		"meta",
		objectValues(bucket, object),
		bytes.NewReader(data),
	)
	if resp != nil {
		err = errors.Join(err, resp.Body.Close())
	}
	return err
}

func (c *storageRESTClient) RenameMeta(bucket, object string) error {
	resp, err := c.call(context.Background(), http.MethodPost, "rename-meta", objectValues(bucket, object), nil)
	if resp != nil {
		err = errors.Join(err, resp.Body.Close())
	}
	return err
}

func (c *storageRESTClient) ReadMeta(bucket, object string) ([]byte, error) {
	resp, err := c.call(context.Background(), http.MethodGet, "meta", objectValues(bucket, object), nil)
	if err != nil {
		return nil, err
	}
	return readStorageRESTBody(resp)
}

func (c *storageRESTClient) WriteUploadMeta(bucket, object, uploadID, name string, data []byte) error {
	values := uploadValues(bucket, object, uploadID, name)
	resp, err := c.call(context.Background(), http.MethodPut, "upload-meta", values, bytes.NewReader(data))
	if resp != nil {
		err = errors.Join(err, resp.Body.Close())
	}
	return err
}

func (c *storageRESTClient) ReadUploadMeta(bucket, object, uploadID, name string) ([]byte, error) {
	resp, err := c.call(
		context.Background(),
		http.MethodGet,
		"upload-meta",
		uploadValues(bucket, object, uploadID, name),
		nil,
	)
	if err != nil {
		return nil, err
	}
	return readStorageRESTBody(resp)
}

func (c *storageRESTClient) DeleteUpload(bucket, object, uploadID string) error {
	resp, err := c.call(
		context.Background(),
		http.MethodDelete,
		"upload",
		uploadValues(bucket, object, uploadID, ""),
		nil,
	)
	if resp != nil {
		err = errors.Join(err, resp.Body.Close())
	}
	return err
}

func (c *storageRESTClient) DeleteObject(bucket, object string) error {
	resp, err := c.call(context.Background(), http.MethodDelete, "object", objectValues(bucket, object), nil)
	if resp != nil {
		err = errors.Join(err, resp.Body.Close())
	}
	return err
}

func (c *storageRESTClient) ListObjects(bucket, prefix string) ([]string, error) {
	resp, err := c.call(
		context.Background(),
		http.MethodGet,
		"objects",
		url.Values{"bucket": {bucket}, "prefix": {prefix}},
		nil,
	)
	if err != nil {
		return nil, err
	}
	var names []string
	data, err := readStorageRESTBody(resp)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, err
	}
	return names, nil
}

func (c *storageRESTClient) readShard(
	ctx context.Context,
	bucket, object, dataDir string,
	partNum int,
	offset int64,
	p []byte,
) (int, error) {
	values := shardValues(bucket, object, dataDir, partNum)
	values.Set("offset", strconv.FormatInt(offset, 10))
	values.Set("length", strconv.Itoa(len(p)))
	resp, err := c.call(ctx, http.MethodGet, "shard", values, nil)
	if err != nil {
		return 0, err
	}
	n, readErr := io.ReadFull(resp.Body, p)
	return n, errors.Join(readErr, resp.Body.Close())
}

type storageRESTShardWriter struct {
	writer   *io.PipeWriter
	done     <-chan error
	once     sync.Once
	closeErr error
}

func (w *storageRESTShardWriter) Write(p []byte) (int, error) {
	return w.writer.Write(p)
}

func (w *storageRESTShardWriter) Close() error {
	w.once.Do(func() {
		w.closeErr = errors.Join(w.writer.Close(), <-w.done)
	})
	return w.closeErr
}

type storageRESTShardReader struct {
	client  *storageRESTClient
	ctx     context.Context
	bucket  string
	object  string
	dataDir string
	partNum int
}

func (r *storageRESTShardReader) ReadAt(p []byte, offset int64) (int, error) {
	n, err := r.client.readShard(r.ctx, r.bucket, r.object, r.dataDir, r.partNum, offset, p)
	if errors.Is(err, io.ErrUnexpectedEOF) {
		err = io.EOF
	}
	return n, err
}

func (*storageRESTShardReader) Close() error { return nil }

type storageRESTFileInfo struct {
	Filename string    `json:"name"`
	Modified time.Time `json:"modified"`
}

func (f storageRESTFileInfo) Name() string       { return f.Filename }
func (f storageRESTFileInfo) Size() int64        { return 0 }
func (f storageRESTFileInfo) Mode() os.FileMode  { return 0o755 }
func (f storageRESTFileInfo) ModTime() time.Time { return f.Modified }
func (f storageRESTFileInfo) IsDir() bool        { return true }
func (f storageRESTFileInfo) Sys() any           { return nil }

func objectValues(bucket, object string) url.Values {
	return url.Values{"bucket": {bucket}, "object": {object}}
}

func shardValues(bucket, object, dataDir string, partNum int) url.Values {
	values := objectValues(bucket, object)
	values.Set("data-dir", dataDir)
	if partNum != 0 {
		values.Set("part", strconv.Itoa(partNum))
	}
	return values
}

func uploadValues(bucket, object, uploadID, name string) url.Values {
	values := objectValues(bucket, object)
	values.Set("upload-id", uploadID)
	if name != "" {
		values.Set("name", name)
	}
	return values
}

func readStorageRESTBody(resp *http.Response) ([]byte, error) {
	data, readErr := io.ReadAll(resp.Body)
	closeErr := resp.Body.Close()
	return data, errors.Join(readErr, closeErr)
}
