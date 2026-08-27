<p align="center">
  <h1 align="center">mini-minio</h1>
  <p align="center">🪣 一个精简、高性能的 S3 兼容对象存储,内置纠删码</p>
  <p align="center">
    <a href="https://github.com/sanbei101/mini-minio/actions/workflows/test.yml"><img src="https://github.com/sanbei101/mini-minio/actions/workflows/test.yml/badge.svg" alt="CI"></a>
    <a href="https://pkg.go.dev/github.com/sanbei101/mini-minio"><img src="https://pkg.go.dev/badge/github.com/sanbei101/mini-minio.svg" alt="Go Reference"></a>
    <a href="https://go.dev/"><img src="https://img.shields.io/badge/Go-1.27-blue.svg" alt="Go Version"></a>
    <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-green.svg" alt="License"></a>
    <a href="https://sanbei101.github.io/mini-minio/"><img src="https://img.shields.io/badge/📖_电子书-Live_Site-purple" alt="Book"></a>
  </p>
</p>

---

**mini-minio** 是 [MinIO](https://github.com/minio/minio) 核心功能的精简实现。保留了纠删码、并行磁盘 I/O、多集合架构等高性能设计,去掉了分布式通信、自愈、加密、IAM 等生产级复杂度--专注于**学习和理解**对象存储的核心原理。

> 如果你想了解 S3 协议、Reed-Solomon 纠删码、高并发磁盘 I/O 是如何协同工作的,这个项目就是为你准备的。

## 📖 配套电子书

本项目附带一本 **电子书**,讲解 MinIO 的核心原理与 mini-minio 的实现细节:

> **[Mini-Minio 开发指南](https://sanbei101.github.io/mini-minio/)** - 从零构建一个轻量级对象存储系统

| 章节 | 内容 |
|------|------|
| **第 1 章 · S3 协议基础** | Bucket / Object / Key 三大概念,AWS SigV4 签名四步流程,API 路由设计 |
| **第 2 章 · 纠删码原理与实现** | Reed-Solomon 算法、Galois 域、流式编解码、multiWriter / parallelReader、缓冲池 |
| **第 3 章 · Bucket 操作** | 并行创建 / 删除 / 列举,Quorum 一致性,与原版 MinIO 的架构对比 |
| **第 4 章 · Object 操作** | PutObject 11 步流程、GetObject 流式解码、Range 请求、元数据投票机制 |
| **第 5 章 · 二级接口** | 分片上传、Presigned URL、认证中间件、Range 下载与纠删码的集成 |
| **第 6 章 · 分布式设计** | Erasure Set 分区、CRC32 路由、跨集合列举、Quorum 机制、write-then-rename |

每章均包含:核心概念讲解 → 源码逐行分析 → 与原版 MinIO 的详细对比表。

**[开始阅读 →](https://sanbei101.github.io/mini-minio/)**

## ✨ 特性

- **S3 兼容 REST API** - 完整支持 10 个核心操作(GetObject / PutObject / DeleteObject / ListObjects / Multipart Upload / Bucket CRUD / Presigned URL)
- **Reed-Solomon 纠删码** - 基于 [klauspost/reedsolomon](https://github.com/klauspost/reedsolomon),数据分片存储到多块磁盘,容忍磁盘故障
- **多集合架构** - 对象通过 CRC32 哈希分配到不同 Erasure Set,实现负载均衡
- **并行磁盘 I/O** - 读写删查全部并行执行,通过 Quorum 机制保证一致性
- **AWS Signature V4** - 支持 Header 认证和 Presigned URL,兼容标准 AWS SDK / CLI
- **流式编解码** - 10 MiB 分块流式编码,pipe-based 并行解码,内存占用可控
- **4K 对齐缓冲池** - channel 实现的有界缓冲池,配合纠删码库的对齐分配
- **HTTP Range 请求** - 支持 `bytes=start-end` 和 `bytes=-suffix` 格式
- **原子元数据写入** - write-then-rename 模式保证 `xl.meta` 一致性

## 🚀 快速开始

### 安装

```bash
git clone https://github.com/sanbei101/mini-minio.git
cd mini-minio
go build -o mini-minio .
```

### 启动

```bash
# 无认证模式(开发/测试用)
./mini-minio --addr :9000 --data ./data

# 带认证
./mini-minio --addr :9000 --data ./data \
  --access-key minioadmin \
  --secret-key minioadmin
```

### 使用 MinIO 客户端测试

```bash
# 安装 mc(如果还没有)
# https://min.io/docs/minio/linux/reference/minio-mc.html

# 配置别名
mc alias set local http://localhost:9000 minioadmin minioadmin

# 创建 bucket
mc mb local/mybucket

# 上传文件
mc cp myfile.txt local/mybucket/

# 下载文件
mc cp local/mybucket/myfile.txt ./downloaded.txt

# 列出对象
mc ls local/mybucket/
```

## 🏗️ 架构

```
                           ┌─────────────────────────────────────────┐
                           │              S3 Client                  │
                           │   (mc / AWS SDK / curl / warp)          │
                           └────────────────┬────────────────────────┘
                                            │ HTTP
                           ┌────────────────▼────────────────────────┐
                           │          Auth Middleware                 │
                           │       (AWS Signature V4)                │
                           └────────────────┬────────────────────────┘
                                            │
                           ┌────────────────▼────────────────────────┐
                           │          S3 API Router                  │
                           │         (gorilla/mux)                   │
                           └────────────────┬────────────────────────┘
                                            │
                           ┌────────────────▼────────────────────────┐
                           │         ObjectLayer Interface           │
                           └────────────────┬────────────────────────┘
                                            │
                    ┌───────────────────────┼───────────────────────┐
                    │                       │                       │
           ┌────────▼────────┐    ┌────────▼────────┐    ┌────────▼────────┐
           │  erasureSets    │    │  Buffer Pool    │    │  Hash Reader    │
           │  (CRC32 路由)   │    │  (4K 对齐)      │    │  (MD5/SHA256)   │
           └────────┬────────┘    └─────────────────┘    └─────────────────┘
                    │
     ┌──────────────┼──────────────┐
     │              │              │
┌────▼─────┐  ┌────▼─────┐  ┌────▼─────┐
│  Set 0   │  │  Set 1   │  │  Set N   │
│ 4+2 纠删 │  │ 4+2 纠删 │  │ 4+2 纠删 │
├──────────┤  ├──────────┤  ├──────────┤
│ D D D D  │  │ D D D D  │  │ D D D D  │
│ P P      │  │ P P      │  │ P P      │
└──────────┘  └──────────┘  └──────────┘
  D = 数据分片    P = 校验分片
```

**数据流:**
1. 客户端发送 S3 请求(带 SigV4 签名)
2. 路由器解析请求,转发到对应的 handler
3. `erasureSets` 根据对象名的 CRC32 哈希选择目标 Set
4. `erasureObjects` 将数据流式编码为 N 个分片,并行写入磁盘
5. 元数据通过 write-then-rename 原子写入 `xl.meta`

## ⚙️ 配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--addr` | `:9000` | 监听地址 |
| `--data` | `./data` | 数据目录 |
| `--data-blocks` | `4` | 每个 Set 的数据分片数 |
| `--parity-blocks` | `2` | 每个 Set 的校验分片数 |
| `--sets` | `1` | Erasure Set 数量 |
| `--access-key` | *(空)* | 访问密钥(空则跳过认证) |
| `--secret-key` | *(空)* | 秘密密钥 |

> 磁盘总数 = `(data-blocks + parity-blocks) × sets`,程序会自动创建对应数量的磁盘目录。

## 📡 API 参考 (s3兼容)

| 操作 | 方法 | 路径 | 说明 |
|------|------|------|------|
| 列出 Bucket | `GET` | `/` | 返回所有 Bucket |
| 创建 Bucket | `PUT` | `/{bucket}/` | 创建新 Bucket |
| 删除 Bucket | `DELETE` | `/{bucket}/` | 删除空 Bucket |
| 检查 Bucket | `HEAD` | `/{bucket}/` | 检查 Bucket 是否存在 |
| 列出对象 | `GET` | `/{bucket}/` | 支持 prefix / delimiter / continuation-token |
| 上传对象 | `PUT` | `/{bucket}/{key}` | 支持 aws-chunked 编码 |
| 下载对象 | `GET` | `/{bucket}/{key}` | 支持 HTTP Range |
| 检查对象 | `HEAD` | `/{bucket}/{key}` | 返回对象元数据 |
| 删除对象 | `DELETE` | `/{bucket}/{key}` | 删除指定对象 |
| 初始化分片上传 | `POST` | `/{bucket}/{key}?uploads` | 返回 uploadId |
| 上传分片 | `PUT` | `/{bucket}/{key}?partNumber=&uploadId=` | 上传单个分片 |
| 完成分片上传 | `POST` | `/{bucket}/{key}?uploadId=` | 合并所有分片 |
| 中止分片上传 | `DELETE` | `/{bucket}/{key}?uploadId=` | 取消上传 |

## 📂 项目结构

```
mini-minio/
├── main.go                        # 入口:参数解析、磁盘初始化、启动服务
├── cmd/
│   ├── api-handlers.go            # HTTP 路由、S3 处理器、中间件
│   ├── erasure-object.go          # 单集合 ObjectLayer 实现、元数据、分片上传
│   ├── erasure-sets.go            # 多集合 ObjectLayer、CRC32 路由
│   ├── object-api-interface.go    # ObjectLayer 接口定义(9 个方法)
│   ├── object-api-datatypes.go    # 数据类型:BucketInfo / ObjectInfo / ...
│   ├── signature-v4.go            # AWS SigV4 认证实现
│   ├── httprange.go               # HTTP Range 解析
│   ├── errors.go                  # 错误类型定义
│   ├── *_test.go                  # 集成测试 + 基准测试
│   └── erasure_bench_test.go      # 性能基准测试
├── internal/
│   ├── erasure/
│   │   ├── erasure.go             # 纠删码引擎:Encode / Decode / multiWriter / parallelReader
│   │   └── errors.go              # ErrWriteQuorum
│   ├── storage/
│   │   └── disk.go                # 磁盘抽象:文件系统操作、xl.meta 读写
│   ├── bpool/
│   │   └── bpool.go               # 4K 对齐的有界缓冲池
│   └── hash/
│       └── reader.go              # 流式 MD5/SHA256 校验
├── copy/                          # MinIO 源码参考(仅用于学习)
├── data/                          # 运行时数据目录
└── .github/workflows/
    ├── test.yml                   # CI:测试 + 基准 + pprof
    └── warp-bench.yml             # CI:warp S3 压力测试
```

## 📊 基准测试

项目内置了全面的基准测试,CI 中会自动运行 [warp](https://github.com/minio/warp) S3 压力测试:

```bash
# 运行所有基准测试
go test -bench=. -benchmem ./cmd/

# 运行特定基准
go test -bench=BenchmarkPutObject -benchmem ./cmd/
go test -bench=BenchmarkGetObject -benchmem ./cmd/
go test -bench=BenchmarkErasure -benchmem ./cmd/
```

**内置基准测试项:**

| 基准测试 | 说明 |
|----------|------|
| `BenchmarkPutObject_SmallFile` | 1KB 文件写入 |
| `BenchmarkPutObject_MediumFile` | 100KB 文件写入 |
| `BenchmarkPutObject_LargeFile` | 1MB 文件写入 |
| `BenchmarkGetObject` | 100KB 文件读取 |
| `BenchmarkPutObject_Parallel` | 并行写入(4+2 配置) |
| `BenchmarkGetObject_Parallel` | 并行读取(4+2 配置) |
| `BenchmarkErasureEncode` | 原始编码(4+2 / 6+2) |
| `BenchmarkErasureDecode` | 原始解码(4+2) |
| `BenchmarkListObjects` | 列举 100 个对象 |
| `BenchmarkDiskWriteComparison` | 2+1 / 4+2 / 6+2 写入对比 |

## 🗺️ 路线图

### ✅ 已完成

- [x] S3 兼容 REST API(10 个核心操作)
- [x] Reed-Solomon 纠删码(流式编解码)
- [x] 多集合架构 + CRC32 路由
- [x] 并行磁盘 I/O + Quorum 一致性
- [x] AWS Signature V4 认证(Header + Presigned URL)
- [x] HTTP Range 请求支持
- [x] 分片上传(内存模式)
- [x] 4K 对齐缓冲池
- [x] 原子元数据写入(write-then-rename)
- [x] 完整的集成测试和基准测试
- [x] CI 自动化测试 + warp 压力测试

### 🔜 计划中

- [ ] 磁盘化分片上传(当前为内存模式,大文件会 OOM)
- [ ] Content-Type 自动检测
- [ ] hashOrder 磁盘分片分布(优化 I/O 均衡)
- [ ] StorageAPI 接口抽象
- [ ] FileInfo 数据类型
- [ ] MessagePack 元数据编码

## 🔄 与 MinIO 的对比

| 特性 | mini-minio | MinIO |
|------|:----------:|:-----:|
| S3 核心 API | ✅ | ✅ |
| 纠删码 | ✅ | ✅ |
| 多集合架构 | ✅ | ✅ |
| AWS SigV4 | ✅ | ✅ |
| 分片上传 | ✅ (内存) | ✅ (磁盘) |
| 分布式模式 | ❌ | ✅ |
| 自愈 (Healing) | ❌ | ✅ |
| 加密 (SSE) | ❌ | ✅ |
| IAM / 策略 | ❌ | ✅ |
| 版本控制 | ❌ | ✅ |
| 事件通知 | ❌ | ✅ |
| 生命周期管理 | ❌ | ✅ |
| 监控指标 | ❌ | ✅ |
| 依赖数量 | **4** | 50+ |
| 代码行数 | **~2000** | ~200K |

> mini-minio 的目标不是替代 MinIO,而是**帮助你理解 MinIO 的核心设计**。

## 🤝 参与贡献

1. Fork 本仓库
2. 阅读 `./copy` 目录中的 MinIO 源码,理解原版实现
3. 在 `cmd/` 或 `internal/` 中实现你的改动
4. 运行代码检查和测试:
   ```bash
   golangci-lint run --fix   # 检查代码质量
   golangci-lint fmt          # 格式化代码
   go test ./...              # 运行所有测试
   ```
5. 提交 Pull Request
