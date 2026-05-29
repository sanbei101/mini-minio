# <<从0开始构建一个mini-minio>>

## 简介

这个项目是我`抄袭`原版的`minio`开源项目写出的一个`mini-minio`,只包含了`minio`的核心功能,去除了比如`分布式部署`,`多租户`,`版本控制`等高级功能,这个项目的目的是为了帮助我(还有其他读者)理解`minio`的核心设计和实现原理,这个项目的AI规范位于`CLAUDE.md`文件中

## 大纲

### S3 协议基础

- S3 协议概述
- 核心概念:Bucket、Object、Key
- 请求签名与认证(SigV4)

### 纠删码(Erasure Coding)原理与实现

### Minio最核心的部件:文件操作 Erasure Object
- Bucket 操作接口
  - CreateBucket
  - ListBuckets
  - DeleteBucket
  - HeadBucket

- Object 操作接口
  - ListObjects
  - GetObject
  - PutObject
  - DeleteObject

### 衍生出的二级接口
- Multipart Upload
- Presigned URL
  - Presigned Get URL
  - Presigned Put URL
- Range下载支持

### 分布式设计: Erasure Set与Quorum机制


## 书写规范

### 语言规范

- **撰写语言**:中文,技术术语保留英文(如 S3、Erasure Coding、Quorum 等)
- **术语一致性**:首次出现时给出中英文对照,如"纠删码(Erasure Coding)"
- **代码注释**:保留英文注释,符合 Go 社区规范

### 格式规范

- **标题层级**:使用 Markdown 标准标题格式(# ## ###)
- **代码块**:使用 Go 语法高亮,标注文件路径
- **文件引用**:使用相对路径,如 `cmd/erasure-object.go`

### 章节结构

每章包含以下部分:

1. **简介**:本章目标与核心概念
2. **核心概念**:原理讲解与设计思想
3. **代码实现**:关键代码片段与完整实现

### 代码展示规范

- **关键代码**:展示核心逻辑,标注文件路径
- **完整实现**:展示完整函数,包含错误处理
- **代码对比**:与原版 MinIO 对比,说明简化点

### 引用规范

- **原版代码**:引用 `copy/` 目录下的原版 MinIO 代码
- **mini-minio 代码**:引用项目根目录下的实现
- **外部资源**:提供链接和简要说明

### 质量要求

- **准确性**:代码与实际实现一致
- **完整性**:覆盖所有核心功能
- **可读性**:逻辑清晰,循序渐进
- **实用性**:包含可运行的示例代码
