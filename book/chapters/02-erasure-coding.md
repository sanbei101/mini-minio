# 第2章: 纠删码 (Erasure Coding) 原理与实现

## 2.1 纠删码到底是干什么的

纠删码说白了就是一种"用空间换可靠性"的技术。它把一份数据拆成 n 个片段,其中 k 个是数据片段,m 个是校验片段 (n = k + m)。只要有任意 k 个片段还在,就能把原始数据完整恢复出来。

跟最朴素的"多存几份副本"比起来,纠删码的存储效率高得多:

| 特性 | 副本复制 | 纠删码 |
|------|----------|--------|
| 存储开销 | 3x (3 副本) | 1.5x (4+2) |
| 容错能力 | 1 个节点故障 | 2 个节点故障 |
| 读取性能 | 高 (可并行读取) | 中等 (需要解码) |
| 写入性能 | 高 (直接写入) | 中等 (需要编码) |

mini-minio 的默认配置是 4 个数据块 + 2 个校验块,总共 6 个分片。这意味着:
- 6 块磁盘分别存一个分片
- 坏了任意 2 块磁盘,数据照样能恢复
- 存储开销是原始数据的 1.5 倍

配置通过 `NewErasureObjects` 传入:

```go
// erasure-sets.go
func NewErasureObjects(diskPaths []string, dataBlocks, parityBlocks int) (ObjectLayer, error) {
    setDriveCount := dataBlocks + parityBlocks
    // 磁盘数必须是 setDriveCount 的整数倍
    if len(diskPaths) == 0 || len(diskPaths)%setDriveCount != 0 {
        return nil, fmt.Errorf("need disk paths in groups of %d, got %d", setDriveCount, len(diskPaths))
    }
    // ...
}
```

## 2.2 Reed-Solomon 算法: 数学原理

Reed-Solomon 是纠删码最常用的算法,mini-minio 用的 `klauspost/reedsolomon` 库就是它的 Go 实现。

### 有限域 (Galois Field)

Reed-Solomon 的所有运算都在 GF(2^8) 有限域里进行,这个域有 256 个元素 (0-255)。有限域里的加法就是 XOR,乘法用预计算的对数/反对数表。为什么要用有限域?因为普通整数运算会产生进位,没法优雅地做多项式插值。有限域里的运算结果还在域内,保证了数学上的封闭性。

### 多项式插值

核心思想是:把数据看成一个多项式的系数,然后在不同点上求值,得到的就是编码片段。任意 k 个点可以唯一确定一个 k-1 次多项式 -- 这就是为什么丢掉 m 个片段还能恢复。

### 编码矩阵

实际编码时用的是范德蒙德矩阵或柯西矩阵。矩阵的每一行对应一个分片,每一列对应原始数据的一小块。`klauspost/reedsolomon` 库内部会自动选择最优的矩阵实现,还支持 AVX2/SSE 指令集加速。

## 2.3 Erasure 结构体: 核心实现

mini-minio 的纠删码核心在 `internal/erasure/erasure.go` 里。`Erasure` 结构体封装了 reedsolomon 编码器:

```go
// internal/erasure/erasure.go
type Erasure struct {
    encoder                  func() reedsolomon.Encoder  // 延迟初始化
    dataBlocks, parityBlocks int
    blockSize                int64
    pool                     *bpool.BytePoolCap
}
```

编码器用的是延迟初始化 -- 不是在创建 `Erasure` 的时候就初始化,而是第一次真正用到的时候才创建:

```go
func New(dataBlocks, parityBlocks int, pool *bpool.BytePoolCap) (Erasure, error) {
    e := Erasure{
        dataBlocks:   dataBlocks,
        parityBlocks: parityBlocks,
        blockSize:    BlockSize,
        pool:         pool,
    }
    var enc reedsolomon.Encoder
    var once sync.Once
    e.encoder = func() reedsolomon.Encoder {
        once.Do(func() {
            var err error
            enc, err = reedsolomon.New(dataBlocks, parityBlocks,
                reedsolomon.WithAutoGoroutines(int(e.ShardSize())))
            if err != nil {
                log.Panic().Err(err).Msg("failed to create reedsolomon encoder")
            }
        })
        return enc
    }
    return e, nil
}
```

`sync.Once` 保证编码器只初始化一次,多个 goroutine 并发调用也安全。`reedsolomon.WithAutoGoroutines` 让库根据分片大小自动决定用多少个 goroutine 并行编码,不需要手动调。

`BlockSize` 是 10 MiB:

```go
const BlockSize = 10 << 20 // 10 MiB
```

每次读数据都是读一整个 BlockSize 大小的块,然后对这个块做纠删码编码。分片大小是 BlockSize 除以数据块数:

```go
func (e *Erasure) ShardSize() int64 {
    return ceilFrac(e.blockSize, int64(e.dataBlocks))
}
```

4+2 配置下,每个分片是 10MiB / 4 = 2.5 MiB。

## 2.4 写入流程: 数据怎么变成分片

当上传一个对象时,`PutObject` 方法负责把数据编码并写到所有磁盘上。整个流程是这样的:

### 第一步: 准备磁盘文件

```go
// erasure-object.go - PutObject
dataDir := uuid.New().String()
writers := make([]io.Writer, len(e.disks))
files := make([]*os.File, len(e.disks))

for i, d := range e.disks {
    f, ferr := d.CreateShardFile(bucket, object, dataDir, 1)
    // ...
    files[i] = f
    writers[i] = f
}
```

每个磁盘上创建一个 `{bucket}/{object}/{dataDir}/part.1` 文件,拿到文件句柄作为 `io.Writer`。

### 第二步: 从缓冲池获取 buffer

```go
var buffer []byte
if e.pool != nil {
    buffer = e.pool.Get()
    defer e.pool.Put(buffer)
} else {
    buffer = make([]byte, erasure.BlockSize)
}
buffer = buffer[:erasure.BlockSize]
```

缓冲池的作用后面会详细说。这里先知道: 一个 10 MiB 的 buffer 被复用来读取数据块。

### 第三步: 计算 ETag 并编码

```go
md5h := md5.New()
tee := io.TeeReader(data, md5h)

n, encErr := enc.Encode(ctx, tee, writers, buffer, writeQuorum)
```

`io.TeeReader` 让数据在被读取的同时算 MD5,一箭双雕。`Encode` 方法在 `erasure.go` 里,它的工作是循环读取数据块,每读一块就编码一次:

```go
// internal/erasure/erasure.go - Encode
func (e *Erasure) Encode(ctx context.Context, src io.Reader, writers []io.Writer, buf []byte, quorum int) (int64, error) {
    mw := &multiWriter{
        writers:     writers,
        writeQuorum: quorum,
        errs:        make([]error, len(writers)),
    }

    var total int64
    for {
        n, err := io.ReadFull(src, buf)
        eof := err == io.EOF || err == io.ErrUnexpectedEOF
        // ...

        shards, encErr := e.EncodeData(buf[:n])
        // ...

        if err := mw.Write(shards); err != nil {
            return 0, err
        }
        total += int64(n)
        if eof {
            break
        }
    }
    return total, nil
}
```

`EncodeData` 内部先调 `enc.Split` 把数据分成 k 个数据分片,再调 `enc.Encode` 生成 m 个校验分片:

```go
func (e *Erasure) EncodeData(data []byte) ([][]byte, error) {
    shards, err := e.encoder().Split(data)
    if err != nil {
        return nil, err
    }
    if err = e.encoder().Encode(shards); err != nil {
        return nil, err
    }
    return shards, nil
}
```

### 第四步: multiWriter 写入磁盘

`multiWriter` 负责把分片写到对应的磁盘上,并且检查写入法定人数 (quorum):

```go
type multiWriter struct {
    writers     []io.Writer
    writeQuorum int
    errs        []error
}

func (mw *multiWriter) Write(blocks [][]byte) error {
    ok := 0
    for i, w := range mw.writers {
        if mw.errs[i] != nil || w == nil {
            continue
        }
        n, err := w.Write(blocks[i])
        if err != nil {
            mw.errs[i] = err
            continue
        }
        if n != len(blocks[i]) {
            mw.errs[i] = io.ErrShortWrite
            continue
        }
        ok++
    }
    if ok < mw.writeQuorum {
        return ErrWriteQuorum
    }
    return nil
}
```

`writeQuorum` 的计算规则是: 如果数据块数等于校验块数,quorum 是 dataBlocks + 1; 否则是 dataBlocks。这样保证写入成功后,至少有足够的分片能恢复数据:

```go
writeQuorum := e.dataBlocks
if e.dataBlocks == e.parityBlocks {
    writeQuorum++
}
```

### 第五步: 元数据写入 (write-then-rename)

数据写完后,需要在每块磁盘上写 `xl.meta`。mini-minio 用了 write-then-rename 模式来保证原子性:

```go
// 先写到临时文件
tmp, err := disk.WriteMetaTmp(bucket, object, &m)
// ...
// 再原子重命名
err = disk.RenameMeta(bucket, object)
```

```go
// storage/disk.go
func (d *Disk) WriteMetaTmp(bucket, object string, meta any) (string, error) {
    data, err := json.Marshal(meta)
    // ...
    tmp := filepath.Join(dir, metaFile+".tmp")  // xl.meta.tmp
    os.WriteFile(tmp, data, 0o644)
    return tmp, nil
}

func (d *Disk) RenameMeta(bucket, object string) error {
    tmp := filepath.Join(dir, metaFile+".tmp")
    dst := filepath.Join(dir, metaFile)
    return os.Rename(tmp, dst)  // 原子操作
}
```

元数据写入也需要达到 quorum,否则会回滚 (删除临时文件):

```go
if writeOK < writeQuorum {
    for _, tmp := range tmpPaths {
        if tmp != "" {
            os.Remove(tmp)
        }
    }
    return ObjectInfo{}, fmt.Errorf("metadata write quorum not met (%d/%d)", writeOK, writeQuorum)
}
```

## 2.5 读取流程: 分片怎么变回数据

读取对象时,`GetObjectNInfo` 方法负责从磁盘读取分片,解码后返回数据。

### 第一步: 读取元数据

```go
// erasure-object.go - GetObjectNInfo
meta, err := e.readMeta(bucket, object)
```

`readMeta` 会从所有磁盘并行读取 `xl.meta`,然后通过投票选出最权威的那份:

```go
func (e *erasureObjects) readMeta(bucket, object string) (*xlMeta, error) {
    // 并行读取所有磁盘的 xl.meta
    metas := make([]*xlMeta, len(e.disks))
    // ... 并发读取 ...

    // 按 ETag + ModTime 投票,选出出现次数最多的
    type metaKey struct {
        etag    string
        modTime time.Time
    }
    counts := map[metaKey]int{}
    // ... 统计票数,选出最佳 ...
}
```

### 第二步: 打开分片文件

```go
readers := make([]io.ReaderAt, len(e.disks))
for i, d := range e.disks {
    rc, _, ferr := d.ReadShardFile(bucket, object, meta.DataDir, 1)
    if ferr == nil {
        if rat, ok := rc.(io.ReaderAt); ok {
            readers[i] = rat
        }
    }
}
```

如果某块磁盘上的分片文件不存在或损坏,对应的 reader 就是 nil,后面解码时会跳过。

### 第三步: 并行读取和解码

解码用的是 `io.Pipe` 实现流式处理 -- 一个 goroutine 负责解码并写入 pipe 的写端,调用者从 pipe 的读端读数据:

```go
pr, pw := io.Pipe()
go func() {
    decErr := enc.Decode(ctx, pw, readers, offset, length, meta.Size)
    pw.CloseWithError(decErr)
    // 关闭所有分片文件
    for _, c := range closers {
        if c != nil {
            c.Close()
        }
    }
}()
return &GetObjectReader{Reader: pr, ObjInfo: objInfo}, nil
```

### Decode 方法内部

`Decode` 方法是整个读取流程的核心。它用 `parallelReader` 并行读取分片,然后逐块解码:

```go
// internal/erasure/erasure.go - Decode
func (e *Erasure) Decode(ctx context.Context, writer io.Writer, readers []io.ReaderAt, offset, length, totalLength int64) error {
    rp := newParallelReader(readers, e, offset, totalLength, e.pool)
    defer rp.Done()

    startBlock := offset / e.blockSize
    endBlock := (offset + length - 1) / e.blockSize

    for block := startBlock; block <= endBlock; block++ {
        bufs, err = rp.Read(bufs)      // 并行读取这个块的所有分片
        e.DecodeDataBlocks(bufs)        // 纠删码解码
        // 从数据分片中拼出原始数据
        var decoded []byte
        for i := 0; i < e.dataBlocks; i++ {
            decoded = append(decoded, bufs[i]...)
        }
        // 裁剪到请求的范围
        writer.Write(decoded[dataStart:dataEnd])
    }
    return nil
}
```

`DecodeDataBlocks` 调用 reedsolomon 的 `ReconstructData` 只恢复数据分片(不恢复校验分片),因为我们的目标是拿到原始数据:

```go
func (e *Erasure) DecodeDataBlocks(data [][]byte) error {
    missing := 0
    for _, b := range data {
        if len(b) == 0 {
            missing++
        }
    }
    if missing == 0 || missing == len(data) {
        return nil  // 全在或全不在,不需要恢复
    }
    return e.encoder().ReconstructData(data)
}
```

### parallelReader: 并行读取的实现

`parallelReader` 是一个比较精巧的设计。它用 channel-trigger 模式来控制并行读取: 先启动 dataBlocks 个 goroutine 去读,如果某个 goroutine 读成功了就通知停止,读失败了就启动下一个:

```go
// channel-trigger 模式
readTriggerCh := make(chan bool, n)

// 先放 dataBlocks 个 trigger,启动 dataBlocks 个 goroutine
for i := 0; i < p.dataBlocks; i++ {
    readTriggerCh <- true
}

for trigger := range readTriggerCh {
    // 检查是否已经有足够的分片
    if p.canDecode(newBuf) {
        break
    }
    if !trigger {
        continue  // false = 不需要更多读取
    }

    go func(idx int) {
        numRead, err := r.ReadAt(p.buf[bufIdx], p.offset)
        if err != nil {
            readTriggerCh <- true  // 读失败,尝试下一块磁盘
            return
        }
        readTriggerCh <- false  // 读成功,不需要更多
    }(readerIndex)
}
```

这样做的好处是: 如果前 dataBlocks 块磁盘都正常,就只读 dataBlocks 个分片; 如果有磁盘挂了,会自动去读其他磁盘的分片,直到凑够 dataBlocks 个。

## 2.6 缓冲池: 减少内存分配

每次编码或解码都需要一个 10 MiB 的 buffer。如果每次都 `make([]byte, 10<<20)`,GC 压力会很大。mini-minio 用缓冲池来复用这些 buffer。

```go
// internal/bpool/bpool.go
type BytePoolCap struct {
    c    chan []byte  // 用 channel 做有界队列
    w    int          // 可用长度
    wcap int          // 实际容量 (4K 对齐)
}
```

创建缓冲池时,`Populate` 会预分配一批 buffer 填满池子:

```go
func NewBytePoolCap(maxSize uint64, width, capwidth int) *BytePoolCap {
    return &BytePoolCap{
        c:    make(chan []byte, maxSize),
        w:    width,
        wcap: capwidth,
    }
}

func (bp *BytePoolCap) Populate() {
    for _, buf := range reedsolomon.AllocAligned(cap(bp.c), bp.wcap) {
        bp.Put(buf[:bp.w])
    }
}
```

`reedsolomon.AllocAligned` 分配的内存是 4K 对齐的 -- 这对磁盘 I/O 性能很重要,因为操作系统的页大小通常是 4K,对齐的内存可以直接用 O_DIRECT 模式读写,避免额外的拷贝。

Get 和 Put 都是非阻塞的:

```go
func (bp *BytePoolCap) Get() []byte {
    select {
    case b := <-bp.c:     // 池里有,直接拿
        return b
    default:              // 池空了,新分配一个
        return reedsolomon.AllocAligned(1, bp.wcap)[0][:bp.w]
    }
}

func (bp *BytePoolCap) Put(b []byte) {
    if cap(b) != bp.wcap {
        return  // 容量不匹配的不收,防止污染池子
    }
    select {
    case bp.c <- b[:bp.w]:  // 池没满,放回去
    default:                // 池满了,丢弃,让 GC 回收
    }
}
```

在 `erasure-sets.go` 里,缓冲池的大小是 1024 个 buffer:

```go
pool := bpool.NewBytePoolCap(1024, erasure.BlockSize, erasure.BlockSize)
pool.Populate()
```

1024 个 10 MiB buffer 意味着预分配了大约 10 GiB 内存。不过实际使用中,同一时刻活跃的请求数远小于 1024,所以大部分 buffer 会待在池子里等待复用。

`parallelReader` 里还有一个优化: 它会尝试从池子里拿一个大 buffer,然后按分片大小切片,而不是为每个分片单独分配:

```go
func newParallelReader(readers []io.ReaderAt, e *Erasure, offset, totalLength int64, pool *bpool.BytePoolCap) *parallelReader {
    // ...
    if pool != nil && pool.WidthCap() >= n*shardSize {
        stash = pool.Get()  // 一个大 buffer,后面切片用
    }
    // ...
}
```

## 2.7 原版 MinIO 与 mini-minio 的区别

### 纠删码配置

原版 MinIO 支持存储类 (Storage Class),可以根据对象大小或策略动态调整 dataBlocks 和 parityBlocks 的比例。还有 AvailabilityOptimized 模式,在磁盘离线时自动提升校验块数量。默认配置是 4+4。

mini-minio 的配置是固定的 4+2,写死在启动参数里,不支持运行时调整。

### 位腐保护 (Bitrot Protection)

原版 MinIO 用 HighwayHash 算法给每个分片文件算校验和,存在 `.meta` 文件里。每次读取时都会验证校验和,发现数据损坏就从其他磁盘恢复。这能防止静默数据腐化 -- 比如磁盘扇区老化导致数据悄悄变坏。

mini-minio 没有这个机制。分片文件直接读写,没有校验和验证。如果某个分片文件悄悄损坏了,mini-minio 不会发现,直到损坏的分片数量超过容错能力,数据才会真正丢失。

### Readahead 和流式处理

原版 MinIO 对大文件 (>128 MiB) 会启用 `readahead.NewReaderBuffer` 做预读,利用操作系统的 read-ahead 策略提升吞吐量。mini-minio 没有这个优化,直接用 `io.ReadFull` 同步读取。

### Inline Data

原版 MinIO 对小对象有个聪明的优化: 如果数据够小,直接存在 `xl.meta` 里,不单独写分片文件。这样减少了一次文件 I/O。mini-minio 没有这个优化,不管多小的对象都会走完整的编码+分片写入流程。

### 多 Part 支持

原版 MinIO 的 Multipart Upload 中,每个 Part 是独立编码的,有自己的分片文件。CompleteMultipartUpload 时只需要组装元数据,不需要拷贝数据。

mini-minio 的 Multipart Upload 把所有 Part 数据存在内存里,CompleteMultipartUpload 时拼成一个完整的 buffer 再走 PutObject。如果上传一个 5 GB 的文件分成 5 个 Part,这 5 GB 数据全在内存里。

### 编码器自检

原版 MinIO 启动时会对 Reed-Solomon 编码器做自检 (self-test),验证编码和解码计算是否正确。mini-minio 没有这个步骤,直接信任 `klauspost/reedsolomon` 库。
