# 第3章: Bucket 操作

## 3.1 ObjectLayer 接口定义

`ObjectLayer` 是 mini-minio 的核心接口,所有存储操作都通过它来定义。跟 Bucket 相关的一共就四个方法:

```go
// cmd/object-api-interface.go
type ObjectLayer interface {
    MakeBucket(ctx context.Context, bucket string) error
    GetBucketInfo(ctx context.Context, bucket string) (BucketInfo, error)
    ListBuckets(ctx context.Context) ([]BucketInfo, error)
    DeleteBucket(ctx context.Context, bucket string) error
    // ...
}
```

## 3.2 数据类型

### BucketInfo

```go
// cmd/object-api-datatypes.go
type BucketInfo struct {
    Name    string    // Bucket 名称
    Created time.Time // 创建时间
    Deleted time.Time // 删除时间(软删除)
}
```

`Deleted` 字段目前在 mini-minio 里没有实际使用,原版 MinIO 用它做软删除,我们暂时保留了这个字段。

### 错误类型

```go
// cmd/errors.go
var (
    ErrBucketNotFound = errors.New("bucket not found")
    ErrBucketExists   = errors.New("bucket already exists")
)
```

## 3.3 CreateBucket

HTTP 层面很简单,就是从 URL 里取出 bucket 名字,调用 `MakeBucket`,如果出错就返回 409 Conflict:

```go
// cmd/api-handlers.go:106
func (a *apiHandlers) CreateBucket(w http.ResponseWriter, r *http.Request) {
    bucket := mux.Vars(r)["bucket"]
    if err := a.obj.MakeBucket(r.Context(), bucket); err != nil {
        writeError(w, http.StatusConflict, "BucketAlreadyExists", err.Error())
        return
    }
    w.Header().Set("Location", "/"+bucket)
    w.WriteHeader(http.StatusOK)
}
```

有意思的是 `MakeBucket` 的错误处理策略:它对所有错误都返回 409,但实际上层只有 `ErrBucketExists` 会触发。其他磁盘错误(比如权限问题)也会被包装成 409,这是一个简化处理。

核心实现在 `erasureObjects.MakeBucket` 里:

```go
// cmd/erasure-object.go:186
func (e *erasureObjects) MakeBucket(ctx context.Context, bucket string) error {
    e.mu.Lock()
    defer e.mu.Unlock()
    errs := make([]error, len(e.disks))
    var wg sync.WaitGroup
    for i, d := range e.disks {
        wg.Add(1)
        go func(idx int, disk *storage.Disk) {
            defer wg.Done()
            err := disk.MakeBucket(bucket)
            if errors.Is(err, storage.ErrBucketExists) {
                err = nil  // 幂等性: 已存在不算错误
            }
            errs[idx] = err
        }(i, d)
    }
    wg.Wait()
    for _, err := range errs {
        if err != nil {
            return err
        }
    }
    return nil
}
```

几个值得注意的地方:

- **并行创建**: 用 goroutine 同时在所有磁盘上创建目录。如果某个盘已经存在这个 bucket,不算错误(幂等性)。
- **全量成功**: 和后面要讲的删除对象不同,这里要求**所有**磁盘都成功才行。只要有任意一块盘出错就返回错误。
- **写锁保护**: `e.mu.Lock()` 保证同一时刻只有一个创建/删除操作在进行,避免并发问题。

底层的 `Disk.MakeBucket` 就是创建一个目录:

```go
// internal/storage/disk.go:31
func (d *Disk) MakeBucket(bucket string) error {
    p := filepath.Join(d.path, bucket)
    if _, err := os.Stat(p); err == nil {
        return ErrBucketExists
    }
    return os.Mkdir(p, 0o755)
}
```

先 `os.Stat` 检查目录是不是已经存在,不存在才用 `os.Mkdir` 创建。权限 0o755 是标准的目录权限。

创建完成后,磁盘上的目录结构长这样:

```
/disk1/my-bucket/
/disk2/my-bucket/
/disk3/my-bucket/
/disk4/my-bucket/
/disk5/my-bucket/
/disk6/my-bucket/
```

## 3.4 ListBuckets

S3 协议要求 ListBuckets 返回一个 XML,格式是 `<ListAllMyBucketsResult>`:

```go
// cmd/api-handlers.go:85
func (a *apiHandlers) ListBuckets(w http.ResponseWriter, r *http.Request) {
    buckets, err := a.obj.ListBuckets(r.Context())
    if err != nil {
        writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
        return
    }
    type bucket struct {
        Name         string `xml:"Name"`
        CreationDate string `xml:"CreationDate"`
    }
    type resp struct {
        XMLName xml.Name `xml:"ListAllMyBucketsResult"`
        Buckets []bucket `xml:"Buckets>Bucket"`
    }
    var bs []bucket
    for _, b := range buckets {
        bs = append(bs, bucket{Name: b.Name, CreationDate: b.Created.Format(time.RFC3339)})
    }
    writeXML(w, http.StatusOK, resp{Buckets: bs})
}
```

时间格式用的是 `time.RFC3339`(比如 `2026-05-29T10:00:00Z`),这是 S3 规范要求的。

核心实现有个比较有意思的点:它要从所有磁盘收集 bucket 列表,然后做去重:

```go
// cmd/erasure-object.go:89
func (e *erasureObjects) listBucketInfos() ([]BucketInfo, error) {
    type result struct {
        infos []BucketInfo
        err   error
    }
    results := make([]result, len(e.disks))
    var wg sync.WaitGroup
    for i, disk := range e.disks {
        wg.Add(1)
        go func(idx int, d *storage.Disk) {
            defer wg.Done()
            infos, err := d.ListBuckets()
            if err != nil {
                results[idx] = result{err: err}
                return
            }
            buckets := make([]BucketInfo, 0, len(infos))
            for _, info := range infos {
                buckets = append(buckets, BucketInfo{
                    Name:    info.Name(),
                    Created: info.ModTime(),
                })
            }
            results[idx] = result{infos: buckets}
        }(i, disk)
    }
    wg.Wait()

    // 去重: 保留最早的创建时间
    bucketByName := map[string]BucketInfo{}
    var firstErr error
    var okDisks int
    for _, r := range results {
        if r.err != nil {
            if firstErr == nil {
                firstErr = r.err
            }
            continue
        }
        okDisks++
        for _, b := range r.infos {
            existing, exists := bucketByName[b.Name]
            if !exists || b.Created.Before(existing.Created) {
                bucketByName[b.Name] = b
            }
        }
    }
    if okDisks == 0 && firstErr != nil {
        return nil, firstErr
    }

    // 按名称排序
    buckets := make([]BucketInfo, 0, len(bucketByName))
    for _, b := range bucketByName {
        buckets = append(buckets, b)
    }
    sort.Slice(buckets, func(i, j int) bool {
        return buckets[i].Name < buckets[j].Name
    })
    return buckets, nil
}
```

去重逻辑:同一个 bucket 在不同磁盘上都存在(因为 MakeBucket 是并行创建的),所以需要按名称合并。合并时保留最早的创建时间,这样返回的信息更准确。

容错策略和 MakeBucket 不一样:这里不要求所有磁盘都成功,只要有一块盘返回了数据就算成功。只有所有盘都失败了才报错。这是合理的,因为 ListBuckets 是只读操作,部分数据总比没有数据好。

底层实现很简单,`Disk.ListBuckets` 就是读目录:

```go
// internal/storage/disk.go:43
func (d *Disk) ListBuckets() ([]os.FileInfo, error) {
    entries, err := os.ReadDir(d.path)
    if err != nil {
        return nil, err
    }
    var infos []os.FileInfo
    for _, e := range entries {
        if e.IsDir() {
            fi, err := e.Info()
            if err == nil {
                infos = append(infos, fi)
            }
        }
    }
    return infos, nil
}
```

`os.ReadDir` 读出磁盘根目录下的所有条目,过滤掉非目录的文件(比如 `.DS_Store` 之类的),只返回目录类型。`e.Info()` 拿到的 `os.FileInfo` 里包含了目录的修改时间,正好用来当作 bucket 的创建时间。

## 3.5 DeleteBucket

DeleteBucket 的 HTTP 层返回 204 No Content(表示成功但没有 body):

```go
// cmd/api-handlers.go:116
func (a *apiHandlers) DeleteBucket(w http.ResponseWriter, r *http.Request) {
    bucket := mux.Vars(r)["bucket"]
    if err := a.obj.DeleteBucket(r.Context(), bucket); err != nil {
        writeError(w, http.StatusNotFound, "NoSuchBucket", err.Error())
        return
    }
    w.WriteHeader(http.StatusNoContent)
}
```

注意这里对所有错误都返回 404,但实际上只有 bucket 不存在时才会触发。其他磁盘错误也会被包装成 404,这和 CreateBucket 处理所有错误为 409 是一个思路。

核心实现和 MakeBucket 结构几乎一模一样:

```go
// cmd/erasure-object.go:226
func (e *erasureObjects) DeleteBucket(ctx context.Context, bucket string) error {
    e.mu.Lock()
    defer e.mu.Unlock()
    var wg sync.WaitGroup
    errs := make([]error, len(e.disks))
    for i, d := range e.disks {
        wg.Add(1)
        go func(idx int, disk *storage.Disk) {
            defer wg.Done()
            err := disk.DeleteBucket(bucket)
            if os.IsNotExist(err) {
                err = nil  // 幂等性: 不存在不算错误
            }
            errs[idx] = err
        }(i, d)
    }
    wg.Wait()
    for _, err := range errs {
        if err != nil {
            return err
        }
    }
    return nil
}
```

同样要求所有磁盘都成功。如果某个盘上 bucket 已经不存在了(可能之前已经删过),也没关系,`os.IsNotExist` 的错误会被忽略。这种幂等性设计在分布式系统里很重要——客户端重试请求时不会因为"已经删过了"而报错。

底层的 `Disk.DeleteBucket` 用的是 `os.Remove`,这个函数只能删除**空目录**。如果 bucket 下还有对象,会返回 `ENOTEMPTY` 错误。这意味着在 mini-minio 里,你不能删除一个非空的 bucket,必须先把里面的东西删干净。

```go
// internal/storage/disk.go:39
func (d *Disk) DeleteBucket(bucket string) error {
    return os.Remove(filepath.Join(d.path, bucket))
}
```

## 3.6 HeadBucket

S3 的 HEAD 请求通常用来检查资源是否存在,不返回 body。HeadBucket 就是检查 bucket 是否存在:

```go
// cmd/api-handlers.go:125
func (a *apiHandlers) HeadBucket(w http.ResponseWriter, r *http.Request) {
    bucket := mux.Vars(r)["bucket"]
    if _, err := a.obj.GetBucketInfo(r.Context(), bucket); err != nil {
        w.WriteHeader(http.StatusNotFound)
        return
    }
    w.WriteHeader(http.StatusOK)
}
```

实现上它调用的是 `GetBucketInfo`,只关心存不存在,不关心返回的 `BucketInfo` 内容。

`GetBucketInfo` 内部调用了 `statBucket` 辅助方法:

```go
// cmd/erasure-object.go:211
func (e *erasureObjects) GetBucketInfo(ctx context.Context, bucket string) (BucketInfo, error) {
    fi, err := e.statBucket(bucket)
    if errors.Is(err, storage.ErrNotFound) {
        return BucketInfo{}, ErrBucketNotFound
    }
    if err != nil {
        return BucketInfo{}, err
    }
    return BucketInfo{Name: bucket, Created: fi.ModTime()}, nil
}
```

`statBucket` 的遍历策略和前面几个方法不太一样:

```go
// cmd/erasure-object.go:72
func (e *erasureObjects) statBucket(bucket string) (os.FileInfo, error) {
    var firstErr error
    for _, disk := range e.disks {
        info, err := disk.StatBucket(bucket)
        if err == nil {
            return info, nil  // 找到即返回
        }
        if !errors.Is(err, storage.ErrNotFound) && firstErr == nil {
            firstErr = err
        }
    }
    if firstErr != nil {
        return nil, firstErr
    }
    return nil, storage.ErrNotFound
}
```

它是**顺序遍历**,不是并行。原因是 stat 操作本身很快(就是 `os.Stat`),没必要开 goroutine。而且它的逻辑是"找到就返回"——只要有一块盘上存在这个 bucket,直接返回成功。

错误处理也值得注意:它区分了 `ErrNotFound` 和其他错误。如果某块盘返回的是"找不到"(可能是那块盘坏了),会继续检查下一块盘。但如果返回的是其他错误(比如 IO 错误),会优先返回这个错误,而不是继续检查。这是因为 IO 错误可能意味着更严重的问题。

底层的 `Disk.StatBucket` 就是一个 `os.Stat`:

```go
// internal/storage/disk.go:60
func (d *Disk) StatBucket(bucket string) (os.FileInfo, error) {
    fi, err := os.Stat(filepath.Join(d.path, bucket))
    if os.IsNotExist(err) {
        return nil, ErrNotFound
    }
    return fi, err
}
```

把标准库的 `os.IsNotExist` 错误转换成了自定义的 `ErrNotFound`,方便上层统一判断。

## 3.7 并发安全

`erasureObjects` 结构体里有一个 `sync.RWMutex`:

```go
type erasureObjects struct {
    disks        []*storage.Disk
    dataBlocks   int
    parityBlocks int
    pool         *bpool.BytePoolCap
    mu           sync.RWMutex  // 保护并发访问
}
```

但并不是所有 bucket 操作都用它。只有**写操作**(MakeBucket、DeleteBucket)会获取写锁,读操作(ListBuckets、GetBucketInfo)完全没有用锁。这在并发安全上其实有点冒险——如果在 ListBuckets 执行的同时有 MakeBucket 在跑,理论上可能读到不一致的状态。不过在 mini-minio 的简化场景下,这种竞态条件的影响可以忽略。

## 3.8 和原版 MinIO 的差异

### 架构层次

原版 MinIO 的 bucket 操作要经过好几层: `erasureServerPools -> s3Peer -> erasureSets -> erasureObjects`。每一层都有自己的职责——`erasureServerPools` 管理多个服务器池,`s3Peer` 处理分布式节点间的通信,`erasureSets` 管理纠删码集合。mini-minio 把这些全部砍掉了,只保留了两层: `erasureObjects -> storage.Disk`。

### 分布式锁 vs 本地锁

原版 MinIO 用的是 `dsync` 分布式锁,确保集群中只有一个节点在执行 bucket 操作:

```go
// 原版 MinIO
lock := er.NewNSLock(minioMetaTmpBucket, bucket+".lck")
lkctx, err := lock.GetLock(ctx, globalOperationTimeout)
defer lock.Unlock(lkctx)
```

mini-minio 直接用 `sync.RWMutex`,只在单进程内有效。

### Bucket 元数据

原版 MinIO 在创建 bucket 时会同时创建一堆元数据配置:版本控制、对象锁定、配额、复制等。这些配置都存储在 `.minio.sys/bucket/` 目录下。mini-minio 完全没有这个机制,bucket 就是一个普通的目录。

### MakeBucket 流程对比

原版 MinIO 的 MakeBucket 做了这些事:
1. 获取分布式锁
2. 验证 bucket 名称(长度、字符等)
3. 检查是否已存在
4. 在所有磁盘上创建目录
5. 创建 bucket 元数据(版本控制、对象锁定等)
6. 保存元数据到 `.minio.sys/bucket/`

mini-minio 的 MakeBucket 就三步:
1. 获取本地互斥锁
2. 在所有磁盘上创建目录
3. 忽略 `ErrBucketExists`

### DeleteBucket 流程对比

原版 MinIO 在删除前会检查 bucket 是否为空:

```go
// 原版 MinIO
if !opts.Force {
    entries, err := er.ListObjects(ctx, bucket, "", "", "/", 1)
    if len(entries.Objects) > 0 || len(entries.Prefixes) > 0 {
        return BucketNotEmpty{Bucket: bucket}
    }
}
```

mini-minio 没有这个检查,直接调用 `os.Remove`。如果目录非空,`os.Remove` 会返回错误,效果类似,但错误信息不一样。

### ListBuckets 缓存

原版 MinIO 对 ListBuckets 做了缓存,结果会缓存 5 秒,避免每次都遍历磁盘。mini-minio 每次都从磁盘读取,在对象数量少的时候性能差异不大。

### GetBucketInfo 元数据丰富

原版 MinIO 的 GetBucketInfo 会从元数据系统中加载版本控制、对象锁定等配置,填充到返回的 BucketInfo 中。mini-minio 只返回 bucket 名称和创建时间。

## 3.9 回顾一下

四个 bucket 操作的设计模式:

- **MakeBucket** 和 **DeleteBucket**: 并行操作所有磁盘,要求全部成功,用写锁保护并发
- **ListBuckets**: 并行读取所有磁盘,做去重和排序,只要有一块盘成功就行
- **HeadBucket**: 顺序遍历磁盘,找到就返回,不需要加锁

写操作用的是"全量成功"策略,读操作用的是"尽量可用"策略。这种不对称的设计在分布式存储里很常见——写操作要保证数据一致性,读操作要保证可用性。
