# mini-minio vs 原版 minio 差异分析报告

## 一、文件结构对比

### 当前 mini-minio 文件树

```
mini-minio/
├── main.go                            (入口，根目录)
├── cmd/
│   ├── api-handlers.go                (路由 + 所有 S3 处理器，合在一个文件)
│   ├── api_test.go
│   ├── erasure_bench_test.go
│   ├── erasure-object.go             (erasureObjects + 内存 multipart)
│   ├── erasure-sets.go               (多 set 路由 + buffer pool 初始化)
│   ├── erasure-sets_test.go
│   ├── errors.go
│   ├── httprange.go
│   ├── object-api-datatypes.go
│   ├── object-api-interface.go
│   ├── object-api-utils.go
│   └── signature-v4.go
├── internal/
│   ├── bpool/
│   │   └── bpool.go                  (BytePoolCap 缓冲池) ✅ 已实现
│   ├── erasure/
│   │   ├── erasure.go                (Erasure + multiWriter + parallelReader)
│   │   └── errors.go
│   ├── hash/
│   │   └── reader.go
│   └── storage/
│       └── disk.go                   (磁盘抽象 + WriteMetaTmp/RenameMeta)
└── copy/                              (原版 minio 参考源码)
```

### 原版 minio 文件树（精简后应保留的部分）

```
minio/
├── cmd/
│   ├── server-main.go                 (入口，在 cmd/ 下)
│   ├── api-router.go                  (路由注册单独文件)
│   ├── object-handlers.go             (对象处理器：Get/Put/Head/Delete/Copy)
│   ├── bucket-handlers.go             (桶处理器：Create/Delete/List/Head)
│   ├── object-multipart-handlers.go   (multipart 处理器)
│   ├── generic-handlers.go            (中间件链)
│   ├── erasure-object.go              (对象层实现)
│   ├── erasure-encode.go              (编码：multiWriter + Encode)
│   ├── erasure-decode.go              (解码：parallelReader + Decode)
│   ├── erasure-sets.go                (多 set 管理)
│   ├── erasure-server-pool.go         (多 pool 管理 + buffer pool 初始化)
│   ├── erasure-common.go              (共用工具：getOnlineDisks 等)
│   ├── erasure-multipart.go           (磁盘 multipart 实现)
│   ├── erasure-metadata.go            (FileInfo -> ObjectInfo 转换)
│   ├── erasure-coding.go              (Erasure 结构体封装)
│   ├── erasure-utils.go               (writeDataBlocks 等)
│   ├── erasure-errors.go
│   ├── storage-interface.go           (StorageAPI 接口，50+ 方法)
│   ├── storage-datatypes.go           (FileInfo, DiskInfo, VolInfo 等)
│   ├── storage-errors.go
│   ├── xl-storage.go                  (本地磁盘实现)
│   ├── xl-storage-format-v2.go        (xl.meta v2 格式)
│   ├── signature-v4.go
│   ├── signature-v4-parser.go
│   ├── signature-v4-utils.go
│   ├── object-api-interface.go
│   ├── object-api-datatypes.go
│   ├── httprange.go
│   ├── globals.go                     (全局变量，含 buffer pool)
│   └── ...
├── internal/
│   ├── bpool/
│   │   ├── bpool.go                   (BytePoolCap 缓冲池)
│   │   └── pool.go                    (泛型 Pool[T])
│   ├── hash/
│   ├── ioutil/                        (ODirect 池、对齐拷贝)
│   ├── disk/                          (directio、fdatasync)
│   └── ...
```

### 结构差异汇总

| 差异点 | mini-minio | 原版 minio | 状态 |
|--------|-----------|-----------|------|
| 入口文件位置 | `main.go` (根目录) | `cmd/server-main.go` | 待调整 |
| 路由注册 | 和处理器合在 `api-handlers.go` | 单独 `api-router.go` | 待调整 |
| 处理器分文件 | 全部合在一个文件 | 按职责分 3 个文件 | 待调整 |
| 磁盘抽象 | `internal/storage/disk.go` 具体结构体 | `cmd/xl-storage.go` + `storage-interface.go` 接口 | 待调整 |
| 编解码 | `internal/erasure/erasure.go` 含 multiWriter + parallelReader | `cmd/erasure-coding.go` + `erasure-encode.go` + `erasure-decode.go` | 部分对齐 |
| multipart | 在 `erasure-object.go` 内存实现 | 单独 `erasure-multipart.go` 磁盘实现 | 待调整 |
| 元数据 | `xlMeta` 在 `erasure-object.go` 内 | 单独 `erasure-metadata.go` + `FileInfo` | 待调整 |
| 缓冲池 | `internal/bpool/bpool.go` ✅ | `internal/bpool/bpool.go` | ✅ 已对齐 |
| 共用工具 | 无单独文件 | `erasure-common.go` + `erasure-utils.go` | 待调整 |

---

## 二、性能差异分析

### 2.1 缓冲池 (Buffer Pool) ✅ 已解决

| 项目 | 之前 | 现在 |
|------|------|------|
| Encode buffer | 每次分配 `make([]byte, 10MB)` | 从 `bpool.BytePoolCap` 池获取，用完归还 |
| Decode shard buffer | 每个 block 分配 `make([]byte, shardSize)` | 从 pool 获取大 buffer 切分，`parallelReader.Done()` 归还 |
| 池实现 | 无 | `BytePoolCap`：channel-based bounded pool，`reedsolomon.AllocAligned` 4K 对齐 |

### 2.2 并行磁盘 I/O ✅ 已解决

| 操作 | 之前 | 现在 |
|------|------|------|
| readMeta | 顺序遍历，first-wins | 并行读所有磁盘 + quorum 投票（ETag+ModTime 计数） |
| PutObject 写 xl.meta | 顺序写 | 并行 WriteMetaTmp + 并行 RenameMeta（write-then-rename） |
| DeleteObject | 顺序删除 | `sync.WaitGroup` 并行删除 |
| MakeBucket / DeleteBucket | 顺序操作 | 并行操作 |
| ListBuckets | 顺序聚合 | 并行读取 + 聚合 |

### 2.3 parallelReader + 并行解码 ✅ 已解决

| 项目 | 之前 | 现在 |
|------|------|------|
| 读 shard | 顺序从每个 disk 读 | `parallelReader` 并发读 `dataBlocks` 个 disk |
| 处理慢盘 | 串行等待 | channel trigger：成功发 false 停止，失败发 true 尝试下一个 |
| buffer 管理 | 每次分配 | 从 pool 获取 stash buffer 切分为 shard buffers |

### 2.4 Readahead 预读 — 已移除

经评估，本地磁盘 + 标准 I/O 场景下 OS page cache 已覆盖预读需求，`klauspost/readahead` 收益有限，已移除。

### 2.5 Write-then-Rename 原子写入 ✅ 已解决

| 项目 | 之前 | 现在 |
|------|------|------|
| xl.meta 写入 | `os.WriteFile` 直接覆盖 | `WriteMetaTmp` 写临时文件 + `RenameMeta` 原子 rename |
| 崩溃安全性 | 中断会留下不完整数据 | 要么旧数据完整，要么新数据完整 |

### 2.6 hashOrder 磁盘分布

| 项目 | mini-minio | 原版 minio |
|------|-----------|-----------|
| shard 分布 | 按磁盘数组下标顺序写 | `hashOrder(object, N)` 生成排列，不同对象分布不同 |
| 效果 | 所有对象的 data shard 都在 disk0-3，parity 在 disk4-5 | 数据均匀分布到所有磁盘 |

**影响**: 在多 set 场景下，如果不用 hashOrder，某些磁盘的写入负载会明显高于其他磁盘。

### 2.7 对比总结

| 优化项 | 状态 | 优先级 |
|--------|------|--------|
| 缓冲池 | ✅ 已完成 | P0 |
| 并行磁盘 I/O | ✅ 已完成 | P1 |
| parallelReader | ✅ 已完成 | P1 |
| Readahead | 已移除（收益有限） | ~~P2~~ |
| Write-then-Rename | ✅ 已完成 | P2 |
| hashOrder | 待实现 | P3 |
| 对齐内存分配 | 待实现 | P3 |

---

## 三、架构模式差异

### 3.1 多 Writer 模式 ✅ 已解决

mini-minio 现已实现 `multiWriter` 结构体，封装多盘写入 + quorum 检查，与原版模式一致。

### 3.2 StorageAPI 接口抽象

**原版**: `StorageAPI` 接口 50+ 方法，两个实现：
- `xlStorage` — 本地磁盘
- `storageRESTClient` — 远程磁盘（REST/grid RPC）

**mini-minio**: `storage.Disk` 是具体结构体，无法替换实现。

### 3.3 FileInfo 数据载体

**原版**: `FileInfo` 是核心数据类型，包含：
- 对象元数据（name, size, modtime, etag）
- Erasure info（data/parity blocks, block size, distribution, shard sizes）
- Parts 信息
- 版本信息
- 内联数据标记
- Checksum

**mini-minio**: `xlMeta` 结构体字段较少，且直接在 `erasure-object.go` 内定义。

### 3.4 元数据格式

**原版**: MessagePack 编码的 `xl.meta` v2 格式：
- 紧凑二进制，体积小
- 支持多版本、内联数据、free version
- 使用 `tinylib/msgp` 库

**mini-minio**: JSON 编码：
- 文本格式，体积大
- 调试方便
- 不支持内联数据

---

## 四、缺失功能清单

### 影响核心功能的缺失

| 功能 | 影响 | 状态 |
|------|------|------|
| 磁盘 multipart | 大文件上传会 OOM | 待实现 |
| readMeta quorum 投票 | 读到脏数据的风险 | ✅ 已解决 |
| write quorum 检查 | xl.meta 写入无 quorum 保证 | ✅ 已解决 |
| hashOrder 分布 | 磁盘负载不均衡 | 待实现 |
| Content-Type 检测 | 总是返回 `application/octet-stream` | 待实现 |
| 原子 rename | crash 不安全 | ✅ 已解决 |

### 不影响核心功能的缺失（按 CLAUDE.md 不需要实现）

- Healing / 自愈
- 分布式模式 (peer 通信)
- Bitrot 保护
- 加密 (SSE)
- IAM / 权限策略
- 事件通知
- 版本控制
- 对象锁定
- Lifecycle
- Metrics / Tracing
- Admin API
- Streaming Signature V4
- Compression
- SFTP
- 批量操作
- 优雅关闭

---

## 五、建议改进路线

### Phase 1: 性能核心 ✅ 已完成

1. ~~创建 `internal/bpool/bpool.go` — BytePoolCap 实现~~ ✅
2. ~~修改 `Erasure.Encode` 签名接受外部 buffer~~ ✅
3. ~~实现 `parallelReader` 并行解码~~ ✅
4. ~~实现并行读取所有磁盘 + quorum 投票~~ ✅
5. ~~`PutObject` xl.meta 写入改为 write-then-rename~~ ✅

### Phase 2: 架构对齐

1. 实现 `hashOrder` 磁盘分布
2. 定义 `StorageAPI` 接口
3. 定义 `FileInfo` 数据类型
4. xl.meta 改用 MessagePack（可选，JSON 调试更方便）

### Phase 3: 文件结构重组（可选）

1. 移动 `main.go` -> `cmd/server-main.go`
2. 拆分 `api-handlers.go`
3. 拆分编解码文件
4. 移动 `internal/storage/` -> `cmd/xl-storage.go`
5. 拆分 multipart 到单独文件
