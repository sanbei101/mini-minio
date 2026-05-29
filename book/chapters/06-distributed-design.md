# 第6章: 分布式设计: Erasure Set 与 Quorum 机制

## 6.1 为什么需要 Erasure Set

当你有 12 块盘的时候,最简单的做法是把 12 块盘全扔进一个纠删码组。但这样做有个问题:一旦其中一块盘出了问题,所有对象的读写都会受影响。

更好的做法是把盘分成几组,每组独立做纠删码。这就是 Erasure Set 的思路。

```
12 块盘,每 6 块一组:

Set 0: disk0, disk1, disk2, disk3, disk4, disk5   (4数据 + 2校验)
Set 1: disk6, disk7, disk8, disk9, disk10, disk11  (4数据 + 2校验)
```

每个 Set 是独立的纠删码域,一个 Set 坏了不影响另一个。对象按名字哈希路由到某个 Set,这样同一个对象的所有分片都在同一个 Set 里。

## 6.2 erasureSets 的结构与初始化

```go
// cmd/erasure-sets.go:22
type erasureSets struct {
    sets          []*erasureObjects
    setDriveCount int
    dataBlocks    int
    parityBlocks  int
    bufferPool    *bpool.BytePoolCap
}
```

`erasureSets` 是面向上层的 ObjectLayer 实现,内部持有多个 `erasureObjects`(每个代表一个 Erasure Set)。`setDriveCount` 记录每个 Set 有多少块盘(等于 `dataBlocks + parityBlocks`)。

初始化逻辑在 `NewErasureObjects` 里:

```go
// cmd/erasure-sets.go:31
func NewErasureObjects(diskPaths []string, dataBlocks, parityBlocks int) (ObjectLayer, error) {
    setDriveCount := dataBlocks + parityBlocks
    if dataBlocks <= 0 || parityBlocks <= 0 {
        return nil, errors.New("data and parity blocks must be positive")
    }
    if len(diskPaths) == 0 || len(diskPaths)%setDriveCount != 0 {
        return nil, fmt.Errorf("need disk paths in groups of %d, got %d", setDriveCount, len(diskPaths))
    }

    pool := bpool.NewBytePoolCap(1024, erasure.BlockSize, erasure.BlockSize)
    pool.Populate()

    setCount := len(diskPaths) / setDriveCount
    sets := make([]*erasureObjects, 0, setCount)
    for i := range setCount {
        start := i * setDriveCount
        set, err := newErasureObjects(diskPaths[start:start+setDriveCount], dataBlocks, parityBlocks, pool)
        if err != nil {
            return nil, err
        }
        sets = append(sets, set)
    }

    return &erasureSets{
        sets:          sets,
        setDriveCount: setDriveCount,
        dataBlocks:    dataBlocks,
        parityBlocks:  parityBlocks,
        bufferPool:    pool,
    }, nil
}
```

几个值得注意的地方:

- 盘数必须是 `setDriveCount` 的整数倍,否则直接报错。比如 4+2 配置,盘数必须是 6 的倍数(6, 12, 18, ...)。
- 缓冲池(`bpool.BytePoolCap`)是全局共享的,1024 个缓冲区,每个大小为 `erasure.BlockSize`。`Populate()` 会预先分配好所有缓冲区,避免运行时分配。
- 每个 Set 分到 `setDriveCount` 块盘,创建一个 `erasureObjects` 实例。

## 6.3 对象路由

对象按名字哈希路由到某个 Set,用的是 `setForObject` 方法:

```go
// cmd/erasure-sets.go:253
func (s *erasureSets) setForObject(object string) *erasureObjects {
    index := int(crc32.ChecksumIEEE([]byte(object)) % uint32(len(s.sets)))
    return s.sets[index]
}
```

CRC32 哈希计算快,分布也还算均匀。同一个对象名总是路由到同一个 Set,这对数据一致性很重要——写入和读取必须走同一个 Set。

对象相关的操作全部委托给对应的 Set:

```go
// cmd/erasure-sets.go:215
func (s *erasureSets) PutObject(ctx context.Context, bucket, object string, data *PutObjReader) (ObjectInfo, error) {
    return s.setForObject(object).PutObject(ctx, bucket, object, data)
}

// cmd/erasure-sets.go:203
func (s *erasureSets) GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec) (*GetObjectReader, error) {
    return s.setForObject(object).GetObjectNInfo(ctx, bucket, object, rs)
}

// cmd/erasure-sets.go:219
func (s *erasureSets) DeleteObject(ctx context.Context, bucket, object string) (ObjectInfo, error) {
    return s.setForObject(object).DeleteObject(ctx, bucket, object)
}

// cmd/erasure-sets.go:211
func (s *erasureSets) GetObjectInfo(ctx context.Context, bucket, object string) (ObjectInfo, error) {
    return s.setForObject(object).GetObjectInfo(ctx, bucket, object)
}
```

每个方法都是一行:算哈希,找到 Set,调方法。路由逻辑和业务逻辑完全分离。

## 6.4 Bucket 操作

Bucket 跟对象不一样——对象可以按名字哈希到某个 Set,但 Bucket 是全局的,必须在所有 Set 上都存在。

### CreateBucket

```go
// cmd/erasure-sets.go:64
func (s *erasureSets) MakeBucket(ctx context.Context, bucket string) error {
    for _, set := range s.sets {
        if err := set.MakeBucket(ctx, bucket); err != nil {
            return err
        }
    }
    return nil
}
```

遍历所有 Set,逐个创建。某个 Set 失败了就直接返回错误——这意味着可能会出现部分 Set 有这个 Bucket、部分没有的不一致状态。原版 MinIO 用分布式锁来避免这个问题,mini-minio 没有这个机制。

### DeleteBucket

```go
// cmd/erasure-sets.go:125
func (s *erasureSets) DeleteBucket(ctx context.Context, bucket string) error {
    for _, set := range s.sets {
        if err := set.DeleteBucket(ctx, bucket); err != nil {
            return err
        }
    }
    return nil
}
```

逻辑和 CreateBucket 一样:逐个 Set 删除,任一失败就返回。

### GetBucketInfo

```go
// cmd/erasure-sets.go:73
func (s *erasureSets) GetBucketInfo(ctx context.Context, bucket string) (BucketInfo, error) {
    var firstErr error
    for _, set := range s.sets {
        info, err := set.GetBucketInfo(ctx, bucket)
        if err == nil {
            return info, nil
        }
        if !errors.Is(err, ErrBucketNotFound) && firstErr == nil {
            firstErr = err
        }
    }
    if firstErr != nil {
        return BucketInfo{}, firstErr
    }
    return BucketInfo{}, ErrBucketNotFound
}
```

这个方法做了个容错处理:某个 Set 上找不到 Bucket 不代表 Bucket 不存在(可能是创建时部分失败),只有所有 Set 都找不到才算真的不存在。非 `ErrBucketNotFound` 的错误会被优先返回。

### ListBuckets

```go
// cmd/erasure-sets.go:90
func (s *erasureSets) ListBuckets(ctx context.Context) ([]BucketInfo, error) {
    bucketByName := map[string]BucketInfo{}
    var firstErr error
    var okSets int

    for _, set := range s.sets {
        buckets, err := set.ListBuckets(ctx)
        if err != nil {
            if firstErr == nil {
                firstErr = err
            }
            continue
        }
        okSets++
        for _, bucket := range buckets {
            existing, exists := bucketByName[bucket.Name]
            if !exists || bucket.Created.Before(existing.Created) {
                bucketByName[bucket.Name] = bucket
            }
        }
    }
    if okSets == 0 && firstErr != nil {
        return nil, firstErr
    }

    buckets := make([]BucketInfo, 0, len(bucketByName))
    for _, bucket := range bucketByName {
        buckets = append(buckets, bucket)
    }
    sort.Slice(buckets, func(i, j int) bool {
        return buckets[i].Name < buckets[j].Name
    })
    return buckets, nil
}
```

从所有 Set 收集 Bucket 列表,按名字去重。去重时保留最早的创建时间——这能处理前面说的部分创建失败的情况。只有所有 Set 都失败了(`okSets == 0`)才返回错误,单个 Set 失败不影响整体结果。

## 6.5 ListObjects

ListObjects 是整个分布式设计里最复杂的操作。因为对象按哈希分散在不同 Set 里,要列出一个 Bucket 下的所有对象,必须遍历所有 Set。

```go
// cmd/erasure-sets.go:134
func (s *erasureSets) ListObjectsV2(
    ctx context.Context,
    bucket, prefix, continuationToken, delimiter string,
    maxKeys int,
    startAfter string,
) (ListObjectsV2Info, error) {
    names, err := s.listObjectNames(bucket, prefix)
    if err != nil {
        return ListObjectsV2Info{}, err
    }

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

    if maxKeys <= 0 || maxKeys > 1000 {
        maxKeys = 1000
    }

    result := ListObjectsV2Info{
        Objects:  []ObjectInfo{},
        Prefixes: []string{},
    }
    seenPrefix := map[string]bool{}

    for i, name := range names {
        if len(result.Objects)+len(result.Prefixes) >= maxKeys {
            result.IsTruncated = true
            result.NextContinuationToken = names[i-1]
            break
        }

        if delimiter != "" {
            rel := strings.TrimPrefix(name, prefix)
            if idx := strings.Index(rel, delimiter); idx >= 0 {
                commonPrefix := prefix + rel[:idx+len(delimiter)]
                if !seenPrefix[commonPrefix] {
                    seenPrefix[commonPrefix] = true
                    result.Prefixes = append(result.Prefixes, commonPrefix)
                }
                continue
            }
        }

        meta, err := s.setForObject(name).readMeta(bucket, name)
        if err != nil {
            continue
        }
        result.Objects = append(result.Objects, ObjectInfo{
            Bucket:      bucket,
            Name:        name,
            Size:        meta.Size,
            ModTime:     meta.ModTime,
            ETag:        meta.ETag,
            ContentType: meta.ContentType,
        })
    }

    return result, nil
}
```

整个流程分三步:

**第一步:收集名字**。调用 `s.listObjectNames` 从所有 Set 收集对象名,去重排序:

```go
// cmd/erasure-sets.go:223
func (s *erasureSets) listObjectNames(bucket, prefix string) ([]string, error) {
    seen := map[string]bool{}
    names := []string{}
    var foundBucket bool

    for _, set := range s.sets {
        setNames, err := set.listObjectNames(bucket, prefix)
        if errors.Is(err, storage.ErrNotFound) {
            continue
        }
        if err != nil {
            return nil, err
        }
        foundBucket = true
        for _, name := range setNames {
            if seen[name] {
                continue
            }
            seen[name] = true
            names = append(names, name)
        }
    }
    if !foundBucket {
        return nil, ErrBucketNotFound
    }

    sort.Strings(names)
    return names, nil
}
```

注意这里跟 `ListBuckets` 的容错策略不同:单个 Set 的非 `ErrNotFound` 错误会直接返回,而不是跳过。这是因为对象列表如果漏掉一个 Set 的数据,结果就是错的。

**第二步:分页和目录模拟**。用 `continuationToken` 或 `startAfter` 定位起始位置,然后逐个处理。遇到 `delimiter` 时,提取公共前缀作为"目录"。

**第三步:读取元数据**。对于每个对象名,用 `s.setForObject(name)` 找到它所属的 Set,然后调 `readMeta` 读取元数据。这里有个潜在的性能问题:如果有 1000 个对象,就要发 1000 次磁盘读取,而且是串行的。

截断判断也有个小细节:当结果数量达到 `maxKeys` 且还有更多数据时,`NextContinuationToken` 设置为最后一个返回的对象名。下次请求带上这个 token,从这个位置继续。

## 6.6 Quorum 机制

Quorum 是分布式存储里保证数据一致性的核心手段。基本思想是:写入时必须成功写到足够多的副本,读取时从多个副本中选最"正确"的那个。

### 写入 Quorum

在 `PutObject` 里,写入 Quorum 的计算规则是:

```go
// cmd/erasure-object.go:273
writeQuorum := e.dataBlocks
if e.dataBlocks == e.parityBlocks {
    writeQuorum++
}
```

大部分情况下 `writeQuorum = dataBlocks`。只有当数据块和校验块数量相等时(比如 4+4),才加 1。

举几个例子:
- 4+2 配置:writeQuorum = 4。至少 4 块盘写成功才能继续。
- 4+4 配置:writeQuorum = 5。数据块和校验块一样多时,需要额外一块来保证冗余。

这个 Quorum 不只是数据写入,元数据写入也用它。在 `PutObject` 里,数据编码写完后,元数据通过 Write-then-Rename 写入所有盘,成功数必须 >= writeQuorum:

```go
// cmd/erasure-object.go:337
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
```

不满足 Quorum 时,清理所有临时文件,返回错误。这样客户端知道写入失败了,不会拿到一个半写成功的对象。

### 删除 Quorum

删除的 Quorum 规则跟写入不一样:

```go
// cmd/erasure-object.go:504
writeQuorum := len(disks)/2 + 1
successCount := len(disks) - failCount

if successCount < writeQuorum {
    return ObjectInfo{}, fmt.Errorf("delete failed: only %d/%d disks succeeded", successCount, len(disks))
}
```

删除用的是 `len(disks)/2 + 1`——简单多数。6 块盘需要 4 块删除成功。为什么不用 `dataBlocks`?因为删除不需要纠删码解码,只要多数盘确认删除就行。

### 元数据读取 Quorum (投票)

读取元数据时,`readMeta` 从所有盘并行读取,然后通过投票选出"正确"的版本:

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

    // Quorum vote: pick the meta that appears most (by ETag + ModTime match).
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

投票的逻辑是这样的:把每份元数据按 `(ETag, ModTime)` 分组计数,出现次数最多的那个就是"正确"版本。

举个例子,6 块盘的情况:
- disk0: ETag="abc", ModTime=T1
- disk1: ETag="abc", ModTime=T1
- disk2: ETag="def", ModTime=T2 (这块盘的数据可能过时了)
- disk3: ETag="abc", ModTime=T1
- disk4: ETag="abc", ModTime=T1
- disk5: ETag="abc", ModTime=T1

ETag="abc" 出现 5 次,ETag="def" 只出现 1 次。选 abc。

这个投票机制能容忍少数盘的数据不一致,但有个前提:正确版本的数量必须超过半数。如果出现 3:3 的平票,结果就是不确定的(取决于遍历顺序)。在实际生产中,这种情况需要更复杂的处理(比如结合写入时间戳),但 mini-minio 没有做这个。

## 6.7 Write-then-Rename

mini-minio 写入元数据用的是 Write-then-Rename 模式:先把元数据写到临时文件,满足 Quorum 后再原子重命名为最终文件。

```go
// cmd/erasure-object.go:317
// Write-then-rename: write tmp files in parallel, then rename atomically.
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
```

所有盘并行写临时文件。每个盘写完后返回临时文件路径。如果某块盘写失败了,`tmpPaths[idx]` 就是空字符串,后续的重命名会跳过它。

写入完成后检查 Quorum,满足了才做重命名:

```go
// cmd/erasure-object.go:353
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
```

这个模式的好处是崩溃安全:如果在写临时文件的过程中进程挂了,临时文件会被忽略,不会出现读到半写数据的情况。只有重命名成功后,新的元数据才对外可见。

## 6.8 数据编码与解码

### 写入流程

`PutObject` 的完整流程是这样的:

1. 创建纠删码编码器 `erasure.New(dataBlocks, parityBlocks, pool)`
2. 为每块盘创建分片文件 `disk.CreateShardFile(bucket, object, dataDir, 1)`
3. 从缓冲池获取缓冲区
4. 用 `io.TeeReader` 包装数据流,一边编码一边算 MD5
5. 调用 `enc.Encode` 流式编码,数据写入所有盘的分片文件
6. 关闭所有分片文件
7. 构造 `xlMeta` 元数据
8. Write-then-Rename 写入元数据

```go
// cmd/erasure-object.go:251
func (e *erasureObjects) PutObject(ctx context.Context, bucket, object string, data *PutObjReader) (ObjectInfo, error) {
    enc, err := erasure.New(e.dataBlocks, e.parityBlocks, e.pool)
    if err != nil {
        return ObjectInfo{}, err
    }

    dataDir := uuid.New().String()
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

    writeQuorum := e.dataBlocks
    if e.dataBlocks == e.parityBlocks {
        writeQuorum++
    }

    var buffer []byte
    if e.pool != nil {
        buffer = e.pool.Get()
        defer e.pool.Put(buffer)
    } else {
        buffer = make([]byte, erasure.BlockSize)
    }
    buffer = buffer[:erasure.BlockSize]

    md5h := md5.New()
    tee := io.TeeReader(data, md5h)

    n, encErr := enc.Encode(ctx, tee, writers, buffer, writeQuorum)
    for _, f := range files {
        f.Close()
    }
    if encErr != nil {
        return ObjectInfo{}, encErr
    }

    etag := hex.EncodeToString(md5h.Sum(nil))
    // ... 构造 xlMeta, Write-then-Rename
}
```

`dataDir` 是一个 UUID,用于标识这次写入的数据目录。每块盘上会创建 `{bucket}/{object}/{dataDir}/part.1` 这样的分片文件。`enc.Encode` 是流式编码,边读边写,不需要把整个对象加载到内存里。

### 读取流程

`GetObjectNInfo` 的读取是写入的逆过程:

```go
// cmd/erasure-object.go:393
func (e *erasureObjects) GetObjectNInfo(
    ctx context.Context,
    bucket, object string,
    rs *HTTPRangeSpec,
) (*GetObjectReader, error) {
    meta, err := e.readMeta(bucket, object)
    if err != nil {
        return nil, err
    }

    enc, err := erasure.New(meta.DataBlocks, meta.ParityBlocks, e.pool)
    if err != nil {
        return nil, err
    }

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

    offset, length := int64(0), meta.Size
    if rs != nil {
        offset, length, err = rs.GetOffsetLength(meta.Size)
        if err != nil {
            return nil, err
        }
    }

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

这里用了 `io.Pipe` 实现流式解码:一个 goroutine 负责解码并写入 pipe 的写端,调用方从 pipe 的读端读数据。这样整个解码过程是流式的,不需要把整个对象解码到内存里。

解码器(`enc.Decode`)接受一个 `offset` 和 `length`,支持只解码对象的一部分。这就是 Range 请求能高效工作的基础——纠删码引擎会跳过不需要的数据块,只解码目标范围。

注意 `readers` 数组里有些元素可能是 `nil`(某块盘的分片文件打开失败)。纠删码引擎能容忍最多 `parityBlocks` 块盘缺失。

## 6.9 并行 I/O 与缓冲池

mini-minio 的所有磁盘操作都是并行的。PutObject 写数据、写元数据、重命名,readMeta 读元数据,DeleteObject 删除数据——都是用 `sync.WaitGroup` + goroutine 并行执行:

```go
var wg sync.WaitGroup
for i, d := range e.disks {
    wg.Add(1)
    go func(idx int, disk *storage.Disk) {
        defer wg.Done()
        // 执行操作
    }(i, d)
}
wg.Wait()
```

并行 I/O 是分布式存储的基本要求。如果 6 块盘串行写,延迟就是单盘的 6 倍。

缓冲池(`bpool.BytePoolCap`)避免了频繁的内存分配和 GC 压力。`Encode` 函数需要一个缓冲区来暂存编码前的数据块,如果每次都 `make([]byte, erasure.BlockSize)`,GC 会很忙。预分配 1024 个缓冲区,用完还回来,循环使用。

```go
var buffer []byte
if e.pool != nil {
    buffer = e.pool.Get()
    defer e.pool.Put(buffer)
} else {
    buffer = make([]byte, erasure.BlockSize)
}
```

## 6.10 与原版 MinIO 的对比

### 架构层次

原版 MinIO 的架构比 mini-minio 多一层:

```
原版:
erasureServerPools (多个服务器池,用于扩展)
└── erasureSets
    └── erasureObjects
        └── StorageAPI (接口,40+ 方法)
            ├── xlStorage (本地存储)
            └── storage-rest-client (远程存储,通过 REST 访问其他节点的磁盘)

mini-minio:
erasureSets
└── erasureObjects
    └── storage.Disk (本地磁盘,14 个方法)
```

原版支持多服务器池(可以不停机扩容)、远程磁盘(通过网络访问其他节点的本地盘)、负载均衡。mini-minio 只有本地磁盘,单机部署。

### 磁盘抽象层

原版的 `StorageAPI` 接口有 40 多个方法,涵盖了磁盘状态查询、在线检测、自愈跟踪、版本管理等。mini-minio 的 `Disk` 结构只有基本的读写操作:

```go
// mini-minio 的 Disk 接口
type Disk struct {
    path string
}
// 主要方法: MakeBucket, DeleteBucket, ListBuckets, StatBucket,
//          CreateShardFile, ReadShardFile, WriteMeta, WriteMetaTmp,
//          RenameMeta, ReadMeta, DeleteObject, ListObjects
```

原版还有 `DiskID`(每个磁盘有唯一标识,用于格式化校验)、`IsOnline()`(磁盘健康检测)、`Healing()`(自愈状态跟踪)等。这些在 mini-minio 里都没有。

### 分布式锁

原版 MinIO 用 `dsync` 库实现跨节点的分布式锁。MakeBucket、DeleteBucket 这类操作会先拿锁,防止并发冲突。`dsync` 支持锁超时、续期、死锁检测。

mini-minio 用的是 `sync.RWMutex`——本地互斥锁,只在单进程内有效。如果部署多个 mini-minio 实例(虽然目前不支持),这个锁就不够用了。

### 自愈机制

原版 MinIO 有完整的自愈(healing)机制:后台任务定期扫描所有对象,发现损坏的数据就从健康副本恢复。还有手动触发的 `mc admin heal` 命令。

mini-minio 没有自愈。磁盘上的数据坏了就是坏了,除非手动修复。在 4+2 配置下,可以容忍 2 块盘同时出问题;超过 2 块,数据就丢了。

### Quorum 实现

原版的 Quorum 更精细:

```go
// 原版 MinIO 的 Quorum 处理
func reduceWriteQuorumErrs(errs []error, quorum int, total int) error {
    // 统计每种错误的数量
    // 如果满足 Quorum,忽略个别错误
    // 如果不满足,返回最常见的错误类型
}
```

原版会统计错误类型分布,如果大部分盘返回的是同一种错误(比如"磁盘满"),就把这个错误作为整体错误返回。mini-minio 的做法简单得多:数成功数,够就行,不够就报错。

### 错误处理

原版有精细的错误分类:`errFileNotFound`、`errFileAccessDenied`、`errFileVersionNotFound`、`errDiskFull` 等,每种错误对应不同的 HTTP 状态码和 S3 错误码。

mini-minio 只有四个自定义错误:

```go
var (
    ErrBucketNotFound  = errors.New("bucket not found")
    ErrObjectNotFound  = errors.New("object not found")
    ErrBucketExists    = errors.New("bucket already exists")
    ErrInvalidArgument = errors.New("invalid argument")
)
```

其余的错误都是直接从底层透传上来的(比如 `storage.ErrNotFound`、`os.ErrNotExist`)。
