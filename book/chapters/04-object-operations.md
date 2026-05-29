# 第4章: Object 操作

## 4.1 ObjectLayer 接口定义

Object 操作是 mini-minio 的核心功能,接口定义了五个方法:

```go
// cmd/object-api-interface.go
type ObjectLayer interface {
    ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string, maxKeys int, startAfter string) (ListObjectsV2Info, error)
    GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec) (*GetObjectReader, error)
    GetObjectInfo(ctx context.Context, bucket, object string) (ObjectInfo, error)
    PutObject(ctx context.Context, bucket, object string, data *PutObjReader) (ObjectInfo, error)
    DeleteObject(ctx context.Context, bucket, object string) (ObjectInfo, error)
}
```

MultipartUpload 的接口没有放在 ObjectLayer 里,而是作为独立的函数存在,后面会专门讲。

## 4.2 数据类型

### ObjectInfo

```go
// cmd/object-api-datatypes.go
type ObjectInfo struct {
    Bucket          string
    Name            string
    ModTime         time.Time
    Size            int64
    IsDir           bool
    ETag            string
    VersionID       string
    ContentType     string
    ContentEncoding string
    Expires         time.Time
    Parts           []ObjectPartInfo
    Checksum        []byte
    Inlined         bool
    DataBlocks      int
    ParityBlocks    int
}
```

mini-minio 实际用到的字段不多,主要是 `Bucket`、`Name`、`Size`、`ModTime`、`ETag`、`ContentType`。其他字段像 `VersionID`、`ContentEncoding`、`Expires`、`Checksum`、`Inlined` 都是从原版 MinIO 保留下来的,目前没有实际使用。

### ObjectPartInfo

每个 Part 的信息,在 MultipartUpload 完成后会记录在 xl.meta 里:

```go
type ObjectPartInfo struct {
    ETag       string            `json:"etag,omitempty"`
    Number     int               `json:"number"`
    Size       int64             `json:"size"`       // 磁盘上的大小(含纠删码冗余)
    ActualSize int64             `json:"actualSize"` // 原始数据大小
    ModTime    time.Time         `json:"modTime"`
    Index      []byte            `json:"index,omitempty"`
    Checksums  map[string]string `json:"crc,omitempty"`
    Error      string            `json:"error,omitempty"`
}
```

注意 `Size` 和 `ActualSize` 的区别: `ActualSize` 是原始数据的字节数,`Size` 是经过纠删码编码后在单块磁盘上实际占用的空间。它们之间的关系是 `Size = ceil(ActualSize / dataBlocks)`,因为纠删码会把数据切成 dataBlocks 份。

### ListObjectsV2Info

```go
type ListObjectsV2Info struct {
    IsTruncated           bool
    ContinuationToken     string
    NextContinuationToken string
    Objects               []ObjectInfo
    Prefixes              []string
}
```

`IsTruncated` 表示结果是否被截断(还有更多对象没返回),`NextContinuationToken` 是下次请求的分页标记。`Prefixes` 用于目录模拟——当设置了 delimiter 时,公共前缀会被收集到这里。

### HTTPRangeSpec

```go
// cmd/httprange.go
type HTTPRangeSpec struct {
    Start          int64
    End            int64
    IsSuffixLength bool
}
```

`IsSuffixLength` 为 true 时,`Start` 表示从末尾算起的字节数(比如 `bytes=-500` 表示最后 500 字节)。为 false 时,`Start` 和 `End` 是绝对偏移量。

### 错误类型

```go
// cmd/errors.go
var (
    ErrObjectNotFound  = errors.New("object not found")
    ErrInvalidArgument = errors.New("invalid argument")
)
```

## 4.3 PutObject

PutObject 是整个 mini-minio 里最复杂的方法,涉及纠删码编码、并行磁盘写入、MD5 计算、元数据原子写入等多个步骤。

HTTP 层面,它从 URL 里取出 bucket 和 object 名字,用 `NewPutObjReader` 包装请求体,然后调用 `PutObject`:

```go
// cmd/api-handlers.go:204
func (a *apiHandlers) PutObject(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    bucket, object := vars["bucket"], vars["object"]

    size := r.ContentLength
    reader, err := NewPutObjReader(r.Body, size)
    if err != nil {
        writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
        return
    }

    info, err := a.obj.PutObject(r.Context(), bucket, object, reader)
    if err != nil {
        writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
        return
    }
    w.Header().Set("ETag", `"`+info.ETag+`"`)
    w.WriteHeader(http.StatusOK)
}
```

成功时返回 ETag(用双引号包裹,这是 S3 规范的要求)。

核心实现可以分成几个阶段来看:

```go
// cmd/erasure-object.go:251
func (e *erasureObjects) PutObject(ctx context.Context, bucket, object string, data *PutObjReader) (ObjectInfo, error) {
    // 1. 创建纠删码编码器
    enc, err := erasure.New(e.dataBlocks, e.parityBlocks, e.pool)
    if err != nil {
        return ObjectInfo{}, err
    }

    // 2. 生成数据目录 UUID
    dataDir := uuid.New().String()

    // 3. 在所有磁盘上创建分片文件
    writers := make([]io.Writer, len(e.disks))
    files := make([]*os.File, len(e.disks))
    for i, d := range e.disks {
        f, ferr := d.CreateShardFile(bucket, object, dataDir, 1)
        if ferr != nil {
            for j := range i {
                files[j].Close()
            }
            return ObjectInfo{}, ferr
        }
        files[i] = f
        writers[i] = f
    }

    // 4. 计算写入 Quorum
    writeQuorum := e.dataBlocks
    if e.dataBlocks == e.parityBlocks {
        writeQuorum++
    }

    // 5. 从缓冲池获取缓冲区
    var buffer []byte
    if e.pool != nil {
        buffer = e.pool.Get()
        defer e.pool.Put(buffer)
    } else {
        buffer = make([]byte, erasure.BlockSize)
    }
    buffer = buffer[:erasure.BlockSize]

    // 6. 计算 MD5 (ETag)
    md5h := md5.New()
    tee := io.TeeReader(data, md5h)

    // 7. 编码并写入
    n, encErr := enc.Encode(ctx, tee, writers, buffer, writeQuorum)
    for _, f := range files {
        f.Close()
    }
    if encErr != nil {
        return ObjectInfo{}, encErr
    }

    // 8. 构建元数据
    etag := hex.EncodeToString(md5h.Sum(nil))
    now := time.Now().UTC()
    contentType := "application/octet-stream"
    meta := xlMeta{
        Name:         object,
        Bucket:       bucket,
        Size:         n,
        ModTime:      now,
        ETag:         etag,
        ContentType:  contentType,
        DataDir:      dataDir,
        DataBlocks:   e.dataBlocks,
        ParityBlocks: e.parityBlocks,
        BlockSize:    erasure.BlockSize,
        Parts:        []ObjectPartInfo{{Number: 1, Size: enc.ShardFileSize(n), ActualSize: n}},
    }

    // 9. 写入元数据 (write-then-rename)
    tmpPaths := make([]string, len(e.disks))
    var wg sync.WaitGroup
    metaErrs := make([]error, len(e.disks))
    for i, d := range e.disks {
        wg.Add(1)
        go func(idx int, disk *storage.Disk) {
            defer wg.Done()
            m := meta
            m.DiskIndex = idx
            tmp, err := disk.WriteMetaTmp(bucket, object, &m)
            if err != nil {
                metaErrs[idx] = err
                return
            }
            tmpPaths[idx] = tmp
        }(i, d)
    }
    wg.Wait()

    // 10. 检查 Quorum
    writeOK := 0
    for _, err := range metaErrs {
        if err == nil {
            writeOK++
        }
    }
    if writeOK < writeQuorum {
        for _, tmp := range tmpPaths {
            if tmp != "" {
                os.Remove(tmp)
            }
        }
        return ObjectInfo{}, fmt.Errorf("metadata write quorum not met (%d/%d)", writeOK, writeQuorum)
    }

    // 11. 原子性重命名
    var renameWg sync.WaitGroup
    renameErrs := make([]error, len(e.disks))
    for i, d := range e.disks {
        if tmpPaths[i] == "" {
            continue
        }
        renameWg.Add(1)
        go func(idx int, disk *storage.Disk) {
            defer renameWg.Done()
            renameErrs[idx] = disk.RenameMeta(bucket, object)
        }(i, d)
    }
    renameWg.Wait()

    return ObjectInfo{
        Bucket:      bucket,
        Name:        object,
        Size:        n,
        ModTime:     now,
        ETag:        etag,
        ContentType: contentType,
    }, nil
}
```

这段代码值得仔细看几个地方:

**UUID 数据目录**: 每个对象的数据都放在一个 UUID 命名的目录里。这样做的好处是,如果上传失败需要重试,新上传的数据会放在新的 UUID 目录下,不会和旧数据冲突。原版 MinIO 也是这么做的。

**缓冲池**: 纠删码编码需要一个 `BlockSize` 大小的缓冲区(10 MiB)。每次都分配这么大的 slice 会产生不少 GC 压力,所以用 `bpool.BytePoolCap` 做了缓冲池复用。如果池子里没有可用的缓冲区,才 fallback 到直接分配。

**MD5 计算**: 用 `io.TeeReader` 包了一层,数据在流向纠删码编码器的同时,也在计算 MD5。编码完成后直接取 MD5 值作为 ETag。这个 ETag 和 S3 的行为一致——就是整个对象的 MD5。

**Write Quorum**: `writeQuorum` 的计算逻辑是 `dataBlocks`,但如果 `dataBlocks == parityBlocks` 则加 1。举个例子,4+4 配置下 writeQuorum 是 5,4+2 配置下 writeQuorum 是 4。这个设计保证了即使有 parityBlocks 块磁盘损坏,仍然有足够的数据可以恢复。

**Write-then-Rename**: 元数据写入分两步——先写到 `xl.meta.tmp` 临时文件,检查 Quorum 通过后,再原子性重命名为 `xl.meta`。这样可以防止写到一半崩溃导致元数据损坏。如果写临时文件就失败了,会清理掉所有已写的临时文件。

底层的三个磁盘操作:

```go
// internal/storage/disk.go:69 - 创建分片文件
func (d *Disk) CreateShardFile(bucket, object, dataDir string, partNum int) (*os.File, error) {
    dir := filepath.Join(d.path, bucket, object, dataDir)
    if err := os.MkdirAll(dir, 0o755); err != nil {
        return nil, err
    }
    return os.Create(filepath.Join(dir, partName(partNum)))
}

// internal/storage/disk.go:108 - 写元数据临时文件
func (d *Disk) WriteMetaTmp(bucket, object string, meta any) (string, error) {
    dir := filepath.Join(d.path, bucket, object)
    if err := os.MkdirAll(dir, 0o755); err != nil {
        return "", err
    }
    data, err := json.Marshal(meta)
    if err != nil {
        return "", err
    }
    tmp := filepath.Join(dir, metaFile+".tmp")
    if err := os.WriteFile(tmp, data, 0o644); err != nil {
        return "", err
    }
    return tmp, nil
}

// internal/storage/disk.go:125 - 原子性重命名
func (d *Disk) RenameMeta(bucket, object string) error {
    dir := filepath.Join(d.path, bucket, object)
    tmp := filepath.Join(dir, metaFile+".tmp")
    dst := filepath.Join(dir, metaFile)
    return os.Rename(tmp, dst)
}
```

`CreateShardFile` 里的 `partName(partNum)` 会生成 `part.1`、`part.2` 这样的文件名。单次 PutObject 只会创建 `part.1`,MultipartUpload 合并后也是写到 `part.1`。

上传完成后,磁盘上的数据布局:

```
my-bucket/
└── my-object/
    ├── xl.meta          # 元数据 (JSON)
    └── a1b2c3d4-e5f6-7890-abcd-ef1234567890/  # 数据目录 (UUID)
        └── part.1       # 纠删码分片数据
```

`xl.meta` 里存的是 JSON 格式的 `xlMeta` 结构体,包含对象名称、大小、ETag、数据目录 UUID、纠删码配置、Part 列表等信息。每块磁盘上都会有一份 `xl.meta`,但每份的 `DiskIndex` 字段不同,记录了这块磁盘在整个纠删码组里的位置。

## 4.4 GetObject

GetObject 的 HTTP 处理器稍微复杂一点,因为它要处理 Range 请求:

```go
// cmd/api-handlers.go:224
func (a *apiHandlers) GetObject(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    bucket, object := vars["bucket"], vars["object"]

    var rs *HTTPRangeSpec
    if rangeHdr := r.Header.Get("Range"); rangeHdr != "" {
        var parseErr error
        rs, parseErr = parseRangeSpec(rangeHdr)
        if parseErr != nil {
            writeError(w, http.StatusBadRequest, "InvalidRange", parseErr.Error())
            return
        }
    }

    objReader, err := a.obj.GetObjectNInfo(r.Context(), bucket, object, rs)
    if err != nil {
        writeError(w, http.StatusNotFound, "NoSuchKey", err.Error())
        return
    }
    defer objReader.Close()

    info := objReader.ObjInfo
    w.Header().Set("Content-Type", info.ContentType)
    w.Header().Set("ETag", `"`+info.ETag+`"`)
    w.Header().Set("Last-Modified", info.ModTime.UTC().Format(http.TimeFormat))

    if rs != nil {
        offset, length, err := rs.GetOffsetLength(info.Size)
        if err != nil {
            writeError(w, http.StatusBadRequest, "InvalidRange", err.Error())
            return
        }
        w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, info.Size))
        w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
        w.WriteHeader(http.StatusPartialContent)
    } else {
        w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
        w.WriteHeader(http.StatusOK)
    }
    written, err := io.Copy(w, objReader)
    if err != nil {
        if r.Context().Err() != nil {
            log.Warn().
                Str("bucket", bucket).
                Str("object", object).
                Err(err).
                Msg("GetObject: Client disconnected mid-download")
        } else {
            log.Error().
                Str("bucket", bucket).
                Str("object", object).
                Int64("written", written).
                Err(err).
                Msg("GetObject: Failed to write response")
        }
        return
    }
}
```

有 Range 请求时返回 206 Partial Content,没有 Range 时返回 200 OK。响应头里的 `Content-Range` 格式是 `bytes 0-499/1000`,表示"返回的是第 0 到 499 字节,总共 1000 字节"。

`io.Copy` 是流式传输,不会把整个对象加载到内存里。如果客户端中途断开连接,r.Context().Err() 会返回非 nil,这时只记一条警告日志就返回了,不会继续浪费资源。

核心实现:

```go
// cmd/erasure-object.go:393
func (e *erasureObjects) GetObjectNInfo(
    ctx context.Context,
    bucket, object string,
    rs *HTTPRangeSpec,
) (*GetObjectReader, error) {
    // 1. 读取元数据 (Quorum 投票)
    meta, err := e.readMeta(bucket, object)
    if err != nil {
        return nil, err
    }

    // 2. 创建纠删码解码器
    enc, err := erasure.New(meta.DataBlocks, meta.ParityBlocks, e.pool)
    if err != nil {
        return nil, err
    }

    // 3. 打开所有磁盘的分片文件
    readers := make([]io.ReaderAt, len(e.disks))
    closers := make([]io.Closer, len(e.disks))
    for i, d := range e.disks {
        rc, _, ferr := d.ReadShardFile(bucket, object, meta.DataDir, 1)
        if ferr == nil {
            if rat, ok := rc.(io.ReaderAt); ok {
                readers[i] = rat
                closers[i] = rc
            }
        }
    }

    // 4. 计算偏移量和长度
    offset, length := int64(0), meta.Size
    if rs != nil {
        offset, length, err = rs.GetOffsetLength(meta.Size)
        if err != nil {
            return nil, err
        }
    }

    // 5. 创建 Pipe 并在 goroutine 中解码
    pr, pw := io.Pipe()
    go func() {
        decErr := enc.Decode(ctx, pw, readers, offset, length, meta.Size)
        pw.CloseWithError(decErr)
        for _, c := range closers {
            if c != nil {
                c.Close()
            }
        }
    }()

    objInfo := ObjectInfo{
        Bucket:      bucket,
        Name:        object,
        Size:        meta.Size,
        ModTime:     meta.ModTime,
        ETag:        meta.ETag,
        ContentType: meta.ContentType,
    }
    return &GetObjectReader{Reader: pr, ObjInfo: objInfo}, nil
}
```

这里面最巧妙的设计是 **io.Pipe 模式**。`GetObjectNInfo` 返回的不是一个已经解码好的数据,而是一个 Pipe 的读端。真正的解码在 goroutine 里异步进行——HTTP handler 调用 `io.Copy(w, objReader)` 时,才会通过 Pipe 驱动 goroutine 里的解码过程。这样整个数据流是流式的,不需要把整个对象缓冲到内存里。

文件关闭也在 goroutine 里完成,因为解码没结束之前文件不能关。

### readMeta: 元数据的 Quorum 投票

```go
// cmd/erasure-object.go:587
func (e *erasureObjects) readMeta(bucket, object string) (*xlMeta, error) {
    metas := make([]*xlMeta, len(e.disks))
    errs := make([]error, len(e.disks))
    var wg sync.WaitGroup
    for i, d := range e.disks {
        wg.Add(1)
        go func(idx int, disk *storage.Disk) {
            defer wg.Done()
            var m xlMeta
            if err := disk.ReadMeta(bucket, object, &m); err != nil {
                errs[idx] = err
                return
            }
            metas[idx] = &m
        }(i, d)
    }
    wg.Wait()

    // Quorum 投票: 选择出现次数最多的元数据 (按 ETag + ModTime 匹配)
    type metaKey struct {
        etag    string
        modTime time.Time
    }
    counts := map[metaKey]int{}
    bestMeta := metas[0]
    for _, m := range metas {
        if m == nil {
            continue
        }
        k := metaKey{etag: m.ETag, modTime: m.ModTime}
        counts[k]++
    }

    var bestCount int
    for _, m := range metas {
        if m == nil {
            continue
        }
        k := metaKey{etag: m.ETag, modTime: m.ModTime}
        if counts[k] > bestCount {
            bestCount = counts[k]
            bestMeta = m
        }
    }
    if bestMeta == nil {
        return nil, ErrObjectNotFound
    }
    return bestMeta, nil
}
```

投票逻辑:并行从所有磁盘读取 `xl.meta`,然后按 `ETag + ModTime` 组合作为 key 来计数。出现次数最多的那份元数据就是"正确"的版本。如果某个磁盘的 `xl.meta` 读不出来(比如磁盘离线),对应的 metas[i] 就是 nil,会被跳过。

这种投票机制比简单的"取第一个成功"要靠谱——即使某块磁盘上的元数据被意外损坏了(和大多数不一致),也能被正确识别出来。

### Range 请求解析

```go
// cmd/erasure-object.go:638
func (rs *HTTPRangeSpec) GetOffsetLength(size int64) (int64, int64, error) {
    if rs.IsSuffixLength {
        start := max(size+rs.Start, 0)
        return start, size - start, nil
    }
    start := rs.Start
    end := rs.End
    if end < 0 || end >= size {
        end = size - 1
    }
    if start > end {
        return 0, 0, errors.New("invalid range")
    }
    return start, end - start + 1, nil
}
```

S3 支持四种 Range 格式:
- `bytes=0-499`: 第 0 到 499 字节,返回 500 字节
- `bytes=500-999`: 第 500 到 999 字节
- `bytes=-500`: 最后 500 字节(`IsSuffixLength = true`)
- `bytes=500-`: 从第 500 字节到末尾(`End = -1`,会被替换成 `size - 1`)

注意 `start > end` 时会返回错误,这是为了防止客户端发来 `bytes=500-100` 这种无效 Range。

### 底层的文件读取

`ReadShardFile` 和 `ReadMeta` 都很简单:

```go
// internal/storage/disk.go:78
func (d *Disk) ReadShardFile(bucket, object, dataDir string, partNum int) (io.ReadCloser, int64, error) {
    p := filepath.Join(d.path, bucket, object, dataDir, partName(partNum))
    f, err := os.Open(p)
    if os.IsNotExist(err) {
        return nil, 0, ErrNotFound
    }
    if err != nil {
        return nil, 0, err
    }
    fi, err := f.Stat()
    if err != nil {
        f.Close()
        return nil, 0, err
    }
    return f, fi.Size(), nil
}

// internal/storage/disk.go:132
func (d *Disk) ReadMeta(bucket, object string, out any) error {
    data, err := os.ReadFile(filepath.Join(d.path, bucket, object, metaFile))
    if os.IsNotExist(err) {
        return ErrNotFound
    }
    if err != nil {
        return err
    }
    return json.Unmarshal(data, out)
}
```

`ReadShardFile` 返回的是 `io.ReadCloser`,但实际类型是 `*os.File`,它同时实现了 `io.ReaderAt` 接口。GetObjectNInfo 里把它断言成 `io.ReaderAt`,这样纠删码解码器就可以随机读取文件的不同位置,而不需要顺序读。

## 4.5 DeleteObject

DeleteObject 的 HTTP 层和其他删除操作一样,成功返回 204 No Content:

```go
// cmd/api-handlers.go:299
func (a *apiHandlers) DeleteObject(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    bucket, object := vars["bucket"], vars["object"]

    if _, err := a.obj.DeleteObject(r.Context(), bucket, object); err != nil {
        writeError(w, http.StatusNotFound, "NoSuchKey", err.Error())
        return
    }
    w.WriteHeader(http.StatusNoContent)
}
```

核心实现比 DeleteBucket 复杂不少:

```go
// cmd/erasure-object.go:450
func (e *erasureObjects) DeleteObject(ctx context.Context, bucket, object string) (ObjectInfo, error) {
    // 1. 先获取对象信息
    info, err := e.GetObjectInfo(ctx, bucket, object)
    if err != nil {
        return ObjectInfo{}, err
    }

    // 2. 获取磁盘列表
    e.mu.Lock()
    disks := e.disks
    e.mu.Unlock()

    // 3. 并行删除
    errs := make([]error, len(disks))
    var wg sync.WaitGroup
    for i, d := range disks {
        wg.Add(1)
        go func(idx int, disk *storage.Disk) {
            defer wg.Done()
            if ctx.Err() != nil {
                errs[idx] = ctx.Err()
                return
            }
            if disk == nil {
                errs[idx] = errors.New("disk not found")
                return
            }
            errs[idx] = disk.DeleteObject(bucket, object)
        }(i, d)
    }

    // 4. 等待完成或上下文取消
    done := make(chan struct{})
    go func() {
        wg.Wait()
        close(done)
    }()
    select {
    case <-ctx.Done():
        return ObjectInfo{}, ctx.Err()
    case <-done:
    }

    // 5. 检查 Quorum
    var failCount int
    for index, err := range errs {
        if err != nil {
            failCount++
            log.Error().
                Err(err).
                Str("bucket", bucket).
                Str("object", object).
                Int("diskIndex", index).
                Msg("disk delete failed")
        }
    }
    writeQuorum := len(disks)/2 + 1
    successCount := len(disks) - failCount
    if successCount < writeQuorum {
        return ObjectInfo{}, fmt.Errorf("delete failed: only %d/%d disks succeeded", successCount, len(disks))
    }

    return info, nil
}
```

和 DeleteBucket 不同的地方:

**先查后删**: DeleteObject 会先调用 `GetObjectInfo` 确认对象存在,然后把对象信息返回给调用者。这和 S3 的行为一致——删除成功后返回被删除对象的元信息。

**上下文取消**: 这里用了一个比较巧妙的方式来支持 context 取消。它把 `wg.Wait()` 放到一个单独的 goroutine 里,然后用 `select` 同时监听 `ctx.Done()` 和 `done` channel。如果客户端取消了请求(比如断开连接),可以尽快返回,不用等所有磁盘都删完。

**Quorum 检查**: 删除成功的磁盘数必须 >= `len(disks)/2 + 1`。这和 PutObject 的 writeQuorum 计算方式不一样——PutObject 用的是 `dataBlocks`,DeleteObject 用的是简单多数。为什么不一样?因为删除操作不需要纠删码,只需要保证"大多数"磁盘删除成功就行。

**错误日志**: 每块失败的磁盘都会记录一条错误日志,包含磁盘索引,方便排查问题。

底层实现用的是 `os.RemoveAll`,它会递归删除整个对象目录,包括 `xl.meta` 和数据目录:

```go
// internal/storage/disk.go:143
func (d *Disk) DeleteObject(bucket, object string) error {
    return os.RemoveAll(filepath.Join(d.path, bucket, object))
}
```

这里有一个潜在的问题:如果删除到一半崩溃了(比如删了 `xl.meta` 但没删数据目录),下次读取这个对象时 `readMeta` 会返回 `ErrNotFound`,但实际上数据目录还在磁盘上占着空间。原版 MinIO 有垃圾回收机制来处理这种情况,mini-minio 暂时没有。

## 4.6 ListObjects

ListObjects 的 HTTP 处理器要解析一堆查询参数,然后构建 S3 规范的 XML 响应:

```go
// cmd/api-handlers.go:134
func (a *apiHandlers) ListObjects(w http.ResponseWriter, r *http.Request) {
    bucket := mux.Vars(r)["bucket"]
    q := r.URL.Query()
    prefix := q.Get("prefix")
    delimiter := q.Get("delimiter")
    contToken := q.Get("continuation-token")
    startAfter := q.Get("start-after")
    maxKeys := 1000
    if s := q.Get("max-keys"); s != "" {
        if n, err := strconv.Atoi(s); err == nil {
            maxKeys = n
        }
    }

    result, err := a.obj.ListObjectsV2(r.Context(), bucket, prefix, contToken, delimiter, maxKeys, startAfter)
    if err != nil {
        writeError(w, http.StatusNotFound, "NoSuchBucket", err.Error())
        return
    }

    type content struct {
        Key          string `xml:"Key"`
        LastModified string `xml:"LastModified"`
        ETag         string `xml:"ETag"`
        Size         int64  `xml:"Size"`
    }
    type commonPrefix struct {
        Prefix string `xml:"Prefix"`
    }
    type resp struct {
        XMLName               xml.Name       `xml:"ListBucketResult"`
        Name                  string         `xml:"Name"`
        Prefix                string         `xml:"Prefix"`
        KeyCount              int            `xml:"KeyCount"`
        MaxKeys               int            `xml:"MaxKeys"`
        IsTruncated           bool           `xml:"IsTruncated"`
        NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
        Contents              []content      `xml:"Contents"`
        CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
    }

    var contents []content
    for i := range result.Objects {
        o := &result.Objects[i]
        contents = append(contents, content{
            Key:          o.Name,
            LastModified: o.ModTime.Format(time.RFC3339),
            ETag:         `"` + o.ETag + `"`,
            Size:         o.Size,
        })
    }
    var cps []commonPrefix
    for _, p := range result.Prefixes {
        cps = append(cps, commonPrefix{Prefix: p})
    }

    writeXML(w, http.StatusOK, resp{
        Name:                  bucket,
        Prefix:                prefix,
        KeyCount:              len(contents) + len(cps),
        MaxKeys:               maxKeys,
        IsTruncated:           result.IsTruncated,
        NextContinuationToken: result.NextContinuationToken,
        Contents:              contents,
        CommonPrefixes:        cps,
    })
}
```

XML 响应里的 `CommonPrefixes` 是目录模拟的关键——当设置了 delimiter(比如 `/`)时,`photos/2024/cat.jpg` 这种路径会产生一个公共前缀 `photos/2024/`,放在 CommonPrefixes 里,而不是放在 Contents 里。这样客户端就像在浏览文件系统的目录一样。

核心实现分两层: `ListObjectsV2` 负责分页和目录模拟,`listObjectNames` 负责从磁盘收集对象名称。

```go
// cmd/erasure-object.go:514
func (e *erasureObjects) ListObjectsV2(
    ctx context.Context,
    bucket, prefix, continuationToken, delimiter string,
    maxKeys int,
    startAfter string,
) (ListObjectsV2Info, error) {
    // 1. 获取所有对象名称
    names, err := e.listObjectNames(bucket, prefix)
    if errors.Is(err, storage.ErrNotFound) {
        return ListObjectsV2Info{}, ErrBucketNotFound
    }
    if err != nil {
        return ListObjectsV2Info{}, err
    }

    // 2. 处理分页
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

    // 3. 限制最大返回数
    if maxKeys <= 0 || maxKeys > 1000 {
        maxKeys = 1000
    }

    // 4. 构建结果
    var objects []ObjectInfo
    var prefixes []string
    seen := map[string]bool{}

    for _, name := range names {
        if len(objects)+len(prefixes) >= maxKeys {
            break
        }

        // 5. 处理 delimiter (目录模拟)
        if delimiter != "" {
            rel := strings.TrimPrefix(name, prefix)
            if idx := strings.Index(rel, delimiter); idx >= 0 {
                cp := prefix + rel[:idx+len(delimiter)]
                if !seen[cp] {
                    seen[cp] = true
                    prefixes = append(prefixes, cp)
                }
                continue
            }
        }

        // 6. 读取元数据
        meta, merr := e.readMeta(bucket, name)
        if merr != nil {
            continue
        }
        objects = append(objects, ObjectInfo{
            Bucket:  bucket,
            Name:    name,
            Size:    meta.Size,
            ModTime: meta.ModTime,
            ETag:    meta.ETag,
        })
    }

    // 7. 判断是否被截断
    result := ListObjectsV2Info{Objects: objects, Prefixes: prefixes}
    if len(objects)+len(prefixes) >= maxKeys && len(names) > maxKeys {
        result.IsTruncated = true
        if len(objects) > 0 {
            result.NextContinuationToken = objects[len(objects)-1].Name
        }
    }
    return result, nil
}
```

分页逻辑: `continuationToken` 和 `startAfter` 都可以用来指定从哪里开始返回。优先级是 continuationToken > startAfter。因为 names 已经排过序了,所以用 `sort.SearchStrings`(二分查找)快速定位到起始位置,时间复杂度 O(log n)。

目录模拟:对每个对象名,去掉 prefix 后看剩余部分是否包含 delimiter。如果包含,截取到 delimiter 出现的位置作为公共前缀。用 `seen` map 去重,避免同一个前缀出现多次。

延迟元数据读取:只对最终要返回的对象才调用 `readMeta` 读取 xl.meta。对于被 delimiter 过滤掉的对象(变成了 prefix),不需要读元数据。这在对象很多但大部分被分组到目录里时,能显著减少磁盘 IO。

截断判断:如果返回的对象数 + 前缀数达到了 maxKeys,且 names 列表里还有更多对象,就设置 `IsTruncated = true`。`NextContinuationToken` 用最后一个返回的对象名作为下次请求的起点。

### listObjectNames: 从所有磁盘收集对象名

```go
// cmd/erasure-object.go:149
func (e *erasureObjects) listObjectNames(bucket, prefix string) ([]string, error) {
    seen := map[string]bool{}
    names := []string{}
    var firstErr error
    var foundBucket bool

    for _, disk := range e.disks {
        diskNames, err := disk.ListObjects(bucket, prefix)
        if errors.Is(err, storage.ErrNotFound) {
            continue
        }
        if err != nil {
            if firstErr == nil {
                firstErr = err
            }
            continue
        }
        foundBucket = true
        for _, name := range diskNames {
            if seen[name] {
                continue
            }
            seen[name] = true
            names = append(names, name)
        }
    }
    if !foundBucket {
        if firstErr != nil {
            return nil, firstErr
        }
        return nil, storage.ErrNotFound
    }

    sort.Strings(names)
    return names, nil
}
```

注意这里遍历磁盘是**顺序的**,不是并行的。这是因为 `disk.ListObjects` 内部要做 `filepath.WalkDir`,如果并行开太多 goroutine 去遍历文件系统,反而可能因为 IO 竞争导致更慢。顺序遍历虽然慢一点,但实现简单,IO 模式也更友好。

去重逻辑:用 `seen` map 记录已经见过的对象名。因为每个对象的数据和元数据分布在所有磁盘上,所以每个磁盘都会返回相同的对象名列表。去重后只保留一份。

容错:如果某块磁盘返回 `ErrNotFound`(bucket 不存在),跳过它。如果返回其他错误,记录下来但继续遍历。只有当所有磁盘都没找到 bucket 时,才返回 `ErrNotFound`。

### Disk.ListObjects: 文件系统遍历

```go
// internal/storage/disk.go:147
func (d *Disk) ListObjects(bucket, prefix string) ([]string, error) {
    dir := filepath.Join(d.path, bucket)
    if _, err := os.Stat(dir); os.IsNotExist(err) {
        return nil, ErrNotFound
    } else if err != nil {
        return nil, err
    }

    var names []string
    err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, err error) error {
        if err != nil {
            return err
        }
        if entry.IsDir() || entry.Name() != metaFile {
            return nil
        }
        objectDir := filepath.Dir(path)
        name, err := filepath.Rel(dir, objectDir)
        if err != nil {
            return err
        }
        name = filepath.ToSlash(name)
        if prefix == "" || len(name) >= len(prefix) && name[:len(prefix)] == prefix {
            names = append(names, name)
        }
        return nil
    })
    if os.IsNotExist(err) {
        return nil, ErrNotFound
    }
    if err != nil {
        return nil, err
    }
    return names, nil
}
```

这个方法的思路很巧妙:它不是直接列出所有对象目录,而是遍历整个 bucket 目录,找所有的 `xl.meta` 文件。找到一个 `xl.meta`,就通过 `filepath.Dir` 拿到它的父目录,再用 `filepath.Rel` 算出相对于 bucket 目录的路径,就是对象名。

比如 `xl.meta` 的完整路径是 `/disk1/my-bucket/photos/cat/xl.meta`,那么:
- `filepath.Dir` 得到 `/disk1/my-bucket/photos/cat`
- `filepath.Rel(dir, objectDir)` 得到 `photos/cat`
- `filepath.ToSlash` 保证路径分隔符是 `/`

前缀过滤也在这个函数里做了,不是在上层做的。`name[:len(prefix)] == prefix` 就是简单的字符串前缀匹配。

## 4.7 HeadObject

HeadObject 和 GetObject 类似,但不返回 body,只返回响应头。客户端通常用它来检查对象是否存在或者获取对象的大小和类型:

```go
// cmd/api-handlers.go:283
func (a *apiHandlers) HeadObject(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    bucket, object := vars["bucket"], vars["object"]

    objInfo, err := a.obj.GetObjectInfo(r.Context(), bucket, object)
    if err != nil {
        w.WriteHeader(http.StatusNotFound)
        return
    }
    w.Header().Set("Content-Type", objInfo.ContentType)
    w.Header().Set("Content-Length", strconv.FormatInt(objInfo.Size, 10))
    w.Header().Set("ETag", `"`+objInfo.ETag+`"`)
    w.Header().Set("Last-Modified", objInfo.ModTime.UTC().Format(http.TimeFormat))
    w.WriteHeader(http.StatusOK)
}
```

实现很简单,就是调 `readMeta` 读元数据然后构造 `ObjectInfo`:

```go
// cmd/erasure-object.go:378
func (e *erasureObjects) GetObjectInfo(ctx context.Context, bucket, object string) (ObjectInfo, error) {
    meta, err := e.readMeta(bucket, object)
    if err != nil {
        return ObjectInfo{}, err
    }
    return ObjectInfo{
        Bucket:      bucket,
        Name:        object,
        Size:        meta.Size,
        ModTime:     meta.ModTime,
        ETag:        meta.ETag,
        ContentType: meta.ContentType,
    }, nil
}
```

`readMeta` 的 Quorum 投票机制在 GetObject 那节已经讲过了,这里不再重复。

## 4.8 MultipartUpload (分片上传)

分片上传是 S3 里处理大文件的标准方式。整个流程分三步:创建上传 -> 逐个上传分片 -> 合并完成。

mini-minio 的分片上传没有放在 ObjectLayer 接口里,而是作为独立的包级函数实现。原因是分片上传本质上是一个临时状态管理,最终合并时还是会调用 `PutObject`。

### 创建上传

```go
// cmd/api-handlers.go:312
func (a *apiHandlers) CreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    bucket, object := vars["bucket"], vars["object"]
    uploadID := newMultipartUpload(bucket, object)

    type resp struct {
        XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
        Bucket   string   `xml:"Bucket"`
        Key      string   `xml:"Key"`
        UploadID string   `xml:"UploadId"`
    }
    writeXML(w, http.StatusOK, resp{Bucket: bucket, Key: object, UploadID: uploadID})
}
```

`newMultipartUpload` 生成一个 UUID 作为 uploadID,然后在内存里创建一个 `multipartUpload` 结构来跟踪这次上传:

```go
// cmd/erasure-object.go:670
func newMultipartUpload(bucket, object string) string {
    id := uuid.New().String()
    multipartMu.Lock()
    multipartUploads[id] = &multipartUpload{
        bucket: bucket,
        object: object,
        id:     id,
        parts:  map[int][]byte{},
        etags:  map[int]string{},
    }
    multipartMu.Unlock()
    return id
}
```

所有未完成的上传都存在全局变量 `multipartUploads` 里,用 `multipartMu` 保护。这意味着如果服务器重启,所有未完成的上传都会丢失。原版 MinIO 会把分片数据写到磁盘的 `.minio.sys/multipart/` 目录下,重启后还能继续。

### 上传分片

```go
// cmd/api-handlers.go:326
func (a *apiHandlers) UploadPart(w http.ResponseWriter, r *http.Request) {
    uploadID := r.URL.Query().Get("uploadId")
    partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
    if err != nil || partNumber < 1 {
        writeError(w, http.StatusBadRequest, "InvalidArgument", "invalid partNumber")
        return
    }

    etag, err := uploadPart(uploadID, partNumber, r.Body)
    if err != nil {
        writeError(w, http.StatusNotFound, "NoSuchUpload", err.Error())
        return
    }
    w.Header().Set("ETag", `"`+etag+`"`)
    w.WriteHeader(http.StatusOK)
}
```

`uploadPart` 把整个分片的数据读到内存里,计算 MD5 作为 ETag:

```go
// cmd/erasure-object.go:684
func uploadPart(uploadID string, partNumber int, r io.Reader) (string, error) {
    multipartMu.Lock()
    up, ok := multipartUploads[uploadID]
    multipartMu.Unlock()
    if !ok {
        return "", fmt.Errorf("upload not found: %s", uploadID)
    }

    data, err := io.ReadAll(r)
    if err != nil {
        return "", err
    }
    h := md5.Sum(data)
    etag := hex.EncodeToString(h[:])

    up.mu.Lock()
    up.parts[partNumber] = data
    up.etags[partNumber] = etag
    up.mu.Unlock()
    return etag, nil
}
```

这里有个明显的简化:所有分片数据都存在内存里(`map[int][]byte`)。如果上传一个 5GB 的文件,分成 5 个 1GB 的分片,那内存里就会占用 5GB。原版 MinIO 的分片是直接写到磁盘上的,内存占用很小。

### 合并完成

```go
// cmd/api-handlers.go:343
func (a *apiHandlers) CompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    bucket, object := vars["bucket"], vars["object"]
    uploadID := r.URL.Query().Get("uploadId")

    type part struct {
        PartNumber int    `xml:"PartNumber"`
        ETag       string `xml:"ETag"`
    }
    type req struct {
        Parts []part `xml:"Part"`
    }
    var body req
    if err := xml.NewDecoder(r.Body).Decode(&body); err != nil {
        writeError(w, http.StatusBadRequest, "MalformedXML", err.Error())
        return
    }

    partNumbers := make([]int, len(body.Parts))
    for i, p := range body.Parts {
        partNumbers[i] = p.PartNumber
    }

    info, err := completeMultipartUpload(r.Context(), a.obj, uploadID, partNumbers)
    if err != nil {
        writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
        return
    }

    type resp struct {
        XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
        Location string   `xml:"Location"`
        Bucket   string   `xml:"Bucket"`
        Key      string   `xml:"Key"`
        ETag     string   `xml:"ETag"`
    }
    writeXML(w, http.StatusOK, resp{
        Location: "/" + bucket + "/" + object,
        Bucket:   bucket,
        Key:      object,
        ETag:     `"` + info.ETag + `"`,
    })
}
```

客户端在 CompleteMultipartUpload 请求的 body 里发送一个 XML,列出所有分片的编号和 ETag。服务端按编号顺序把所有分片拼接起来,然后调用 `PutObject` 写入:

```go
// cmd/erasure-object.go:706
func completeMultipartUpload(
    ctx context.Context,
    ol ObjectLayer,
    uploadID string,
    partNumbers []int,
) (ObjectInfo, error) {
    multipartMu.Lock()
    up, ok := multipartUploads[uploadID]
    multipartMu.Unlock()
    if !ok {
        return ObjectInfo{}, fmt.Errorf("upload not found: %s", uploadID)
    }

    up.mu.Lock()
    var buf bytes.Buffer
    for _, n := range partNumbers {
        p, exists := up.parts[n]
        if !exists {
            up.mu.Unlock()
            return ObjectInfo{}, fmt.Errorf("part %d not found", n)
        }
        buf.Write(p)
    }
    up.mu.Unlock()

    r, err := NewPutObjReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
    if err != nil {
        return ObjectInfo{}, err
    }
    info, err := ol.PutObject(ctx, up.bucket, up.object, r)
    if err != nil {
        return ObjectInfo{}, err
    }

    multipartMu.Lock()
    delete(multipartUploads, uploadID)
    multipartMu.Unlock()
    return info, nil
}
```

这个实现有一个设计上的取舍:它把所有分片拼成一个完整的对象,然后走 `PutObject` 的标准流程。这意味着:
1. 所有分片数据必须都在内存里(`bytes.Buffer`)
2. 合并后磁盘上只有一个 `part.1`,不像原版 MinIO 那样每个分片是独立的 part 文件
3. ETag 是合并后整个对象的 MD5,不是分片 ETag 的拼接

原版 MinIO 的 MultipartUpload 会保留每个分片作为独立的 part 文件,`xl.meta` 里的 Parts 数组记录了每个 part 的位置和大小。读取时可以按 part 读取,支持随机访问单个分片。

### 放弃上传

```go
// cmd/erasure-object.go:746
func abortMultipartUpload(uploadID string) {
    multipartMu.Lock()
    delete(multipartUploads, uploadID)
    multipartMu.Unlock()
}
```

放弃上传就是从内存 map 里删掉,没有其他清理工作(因为数据都在内存里)。

## 4.9 和原版 MinIO 的差异

### PutObject

原版 MinIO 的 PutObject 有 15 个步骤,mini-minio 精简到了 9 个。主要砍掉了:
- **分布式锁**: 原版用 `dsync` 做分布式命名空间锁,mini-minio 没有加锁(单进程场景不需要)
- **前置条件检查**: 原版支持 `If-Match`、`If-None-Match` 等条件上传
- **存储类**: 原版可以根据存储类动态调整纠删码配置
- **Inline Data**: 原版对小对象(默认 128KiB 以下)会把数据直接存在 xl.meta 里,不创建数据文件,读取时更快
- **Readahead**: 原版对大文件(>128MiB)使用预读缓冲,减少磁盘 IO 次数
- **位腐保护**: 原版用 HighwayHash 计算每个分片的校验和,防止磁盘静默损坏
- **临时目录**: 原版先把数据写到 `minioMetaTmpBucket` 临时位置,成功后再 rename 到目标位置;mini-minio 直接写到目标位置

### GetObject

原版 MinIO 的 GetObject 会获取读锁,防止并发的写操作。mini-minio 没有加锁。原版还支持删除标记检查(版本控制功能)和远程对象(过渡存储功能),mini-minio 都没有。

不过 Pipe 模式是一样的:都用 `io.Pipe` 实现流式解码,避免把整个对象加载到内存。

### DeleteObject

原版 MinIO 的 DeleteObject 支持生命周期检查(自动过期)、前缀批量删除、版本化删除标记等功能。mini-minio 就是先查再删,简单直接。

原版删除时会把数据重命名到一个"回收站"目录(过期后才真正删除),mini-minio 直接 `os.RemoveAll` 彻底删除。

### ListObjects

原版 MinIO 有一个叫 Metacache 的系统,会缓存目录遍历的结果到磁盘上,下次 ListObjects 可以直接复用。mini-minio 每次都从头遍历。

### 元数据格式

原版 MinIO 用的是 msgpack 二进制格式,头部是 `"XL2 " + version`。支持多版本、删除标记、Inline Data 标志、位腐校验和算法、压缩索引等。mini-minio 用 JSON,结构简单,可读性好,但体积更大,解析更慢。

### MultipartUpload

原版 MinIO 的分片数据直接写到磁盘,每个分片是独立的 part 文件。mini-minio 把分片全部存在内存里,合并时拼成一个完整的对象再走 PutObject。这是 mini-minio 目前最大的简化之一——上传大文件时内存占用会很高。

## 4.10 回顾一下

Object 操作的设计可以总结成几个模式:

**写操作(PutObject)**:并行写所有磁盘 -> 检查 writeQuorum -> 写元数据临时文件 -> 检查元数据 Quorum -> 原子 rename

**读操作(GetObject)**:并行读元数据 -> Quorum 投票选正确版本 -> 并行打开分片文件 -> 流式解码(Pipe 模式)

**删除操作(DeleteObject)**:先查再删 -> 并行删除 -> 检查删除 Quorum

**列表操作(ListObjects)**:从所有磁盘收集对象名 -> 去重排序 -> 分页 -> 按需读取元数据

**分片上传(MultipartUpload)**:创建上传(内存) -> 逐个上传分片(内存) -> 合并后走 PutObject

Quorum 机制贯穿始终:写的时候要求 >= writeQuorum 块磁盘成功,读的时候用投票选出现次数最多的版本,删的时候要求 >= n/2+1 块磁盘成功。这样即使有几块磁盘挂了,系统仍然能正常工作。
