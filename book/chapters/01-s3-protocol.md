# 第1章: S3 协议基础

## 1.1 S3 协议是什么

Amazon S3 (Simple Storage Service) 是 AWS 提供的对象存储服务,它定义了一套标准的 RESTful API 来管理对象(也就是文件)。mini-minio 要做的事就是把这套 API 的核心子集实现出来。

S3 的核心思想其实挺简单的,就三个东西:
- **Bucket**: 命名空间,你可以把它理解成文件系统的根目录
- **Object**: 存储的基本单位,由 Key(路径) + Data(数据) + Metadata(元信息) 组成
- **Key**: 对象在 Bucket 中的唯一标识,支持 `/` 分隔符来模拟目录结构

## 1.2 Bucket、Object、Key 到底长什么样

### Bucket

Bucket 是对象的容器。在 mini-minio 中,每个 Bucket 对应每块磁盘上的一个目录:

```
/disk1/my-bucket/
/disk2/my-bucket/
/disk3/my-bucket/
/disk4/my-bucket/
/disk5/my-bucket/
/disk6/my-bucket/
```

这里 6 块磁盘是因为默认配置是 4 数据块 + 2 校验块。磁盘数量必须是 `dataBlocks + parityBlocks` 的整数倍,否则启动会报错:

```go
// erasure-sets.go
setDriveCount := dataBlocks + parityBlocks
if len(diskPaths) == 0 || len(diskPaths)%setDriveCount != 0 {
    return nil, fmt.Errorf("need disk paths in groups of %d, got %d", setDriveCount, len(diskPaths))
}
```

Bucket 的命名规则:
- 全局唯一
- 3-63 个字符
- 只能包含小写字母、数字和连字符
- 不能以连字符开头或结尾

### Object

Object 的存储结构是这样的:

```
my-bucket/
└── photos/
    └── 2024/
        └── 01/
            └── image.jpg/
                ├── xl.meta          # 元数据 (JSON 格式)
                └── a1b2c3d4/        # 数据目录 (UUID)
                    └── part.1       # 数据分片
```

这里有个细节: `xl.meta` 在 mini-minio 中用的是 JSON 格式,而不是原版 MinIO 的 MessagePack 二进制格式。每块磁盘上都会写一份 `xl.meta`,里面记录了对象的名字、大小、ETag、数据目录 UUID、纠删码配置等信息:

```go
// erasure-object.go
type xlMeta struct {
    Name         string            `json:"name"`
    Bucket       string            `json:"bucket"`
    Size         int64             `json:"size"`
    ModTime      time.Time         `json:"modTime"`
    ETag         string            `json:"etag"`
    ContentType  string            `json:"contentType"`
    DataDir      string            `json:"dataDir"`
    DataBlocks   int               `json:"dataBlocks"`
    ParityBlocks int               `json:"parityBlocks"`
    BlockSize    int64             `json:"blockSize"`
    Parts        []ObjectPartInfo  `json:"parts"`
    DiskIndex    int               `json:"diskIndex"`
}
```

数据文件放在 `DataDir` (一个 UUID 目录) 下面,文件名是 `part.1`、`part.2` 这样递增的。不过 mini-minio 目前只支持单 part,所以基本只会看到 `part.1`。

### Key

Key 就是对象的路径,比如 `photos/2024/01/image.jpg`。在同一个 Bucket 内 Key 是唯一的,区分大小写,最大 1024 字节。

## 1.3 认证: SigV4 签名怎么玩

S3 用 AWS Signature Version 4 做请求认证。mini-minio 支持两种认证方式,由 `authMiddleware` 统一处理:

```go
// api-handlers.go
func authMiddleware(creds Credentials, next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        var err error
        switch {
        case r.URL.Query().Get("X-Amz-Signature") != "":
            // Presigned URL 请求,签名在查询参数里
            if err = r.ParseForm(); err == nil {
                err = verifyPresignedAuth(r, creds)
            }
        case r.Header.Get("Authorization") != "":
            // 普通请求,签名在 Authorization 头里
            err = verifyHeaderAuth(r, creds)
        default:
            err = errors.New("missing authentication")
        }
        if err != nil {
            writeError(w, http.StatusForbidden, "AccessDenied", err.Error())
            return
        }
        next.ServeHTTP(w, r)
    })
}
```

如果没配置 AccessKey (空字符串),认证中间件会直接跳过,这对本地开发测试很方便。

### Header 认证

普通的 HTTP 请求把签名放在 `Authorization` 头里:

```http
GET /my-bucket/my-object HTTP/1.1
Host: minio.example.com
Authorization: AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=1234567890abcdef
```

### Presigned URL

Presigned URL 把签名放在查询参数里,这样任何人拿到这个 URL 就可以直接访问,不需要额外的认证:

```
https://minio.example.com/my-bucket/my-object?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...&X-Amz-Expires=3600&X-Amz-Signature=...
```

mini-minio 生成 Presigned URL 的代码很直接:

```go
// signature-v4.go
func PresignURL(baseURL, method, bucket, object, accessKey, secretKey string, expiry time.Duration) string {
    t := time.Now().UTC()
    // 构建查询参数
    q := url.Values{}
    q.Set("X-Amz-Algorithm", signV4Algorithm)
    q.Set("X-Amz-Credential", accessKey+"/"+scope(t))
    q.Set("X-Amz-Date", t.Format(iso8601Format))
    q.Set("X-Amz-Expires", strconv.Itoa(int(expiry.Seconds())))
    q.Set("X-Amz-SignedHeaders", "host")
    // ... 构建 Canonical Request, 计算签名 ...
    q.Set("X-Amz-Signature", sig)
    return baseURL + path + "?" + q.Encode()
}
```

### 签名验证的完整流程

不管是 Header 认证还是 Presigned URL,验证逻辑都一样,分四步:

**第一步: 构建 Canonical Request**

把 HTTP 请求的各个部分按固定格式拼起来:

```
CanonicalRequest =
  HTTPRequestMethod + '\n' +
  CanonicalURI + '\n' +
  CanonicalQueryString + '\n' +
  CanonicalHeaders + '\n' +
  SignedHeaders + '\n' +
  HexEncode(Hash(RequestPayload))
```

mini-minio 里对应的代码:

```go
canonReq := strings.Join([]string{
    r.Method,
    r.URL.EscapedPath(),
    canonicalQueryString(r.URL.Query(), false),
    canonHdr,
    signedStr,
    payloadHash,
}, "\n")
```

这里的 `canonicalQueryString` 会对查询参数按字母序排序并 URL 编码。对于 Presigned URL,签名参数 `X-Amz-Signature` 会被排除在外(通过 `excludeSig` 参数控制)。

**第二步: 创建 String to Sign**

```
StringToSign =
  Algorithm + '\n' +
  RequestDateTime + '\n' +
  CredentialScope + '\n' +
  HexEncode(Hash(CanonicalRequest))
```

**第三步: 计算签名密钥**

通过四层 HMAC-SHA256 派生:

```go
// signature-v4.go
func signingKey(secretKey string, t time.Time) []byte {
    date := hmacSHA256([]byte("AWS4"+secretKey), []byte(t.Format(yyyymmdd)))
    reg := hmacSHA256(date, []byte(region))
    svc := hmacSHA256(reg, []byte(service))
    return hmacSHA256(svc, []byte("aws4_request"))
}
```

**第四步: 比较签名**

用 `crypto/subtle.ConstantTimeCompare` 做常量时间比较,防止时序攻击:

```go
if subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) != 1 {
    return errors.New("signature mismatch")
}
```

## 1.4 API 路由

mini-minio 用 `gorilla/mux` 做路由,所有路由定义都在 `NewRouter` 里:

```go
// api-handlers.go
func NewRouter(obj ObjectLayer, creds Credentials) http.Handler {
    r := mux.NewRouter()
    api := apiHandlers{obj: obj, creds: creds}

    // Bucket 操作
    r.Methods("GET").Path("/").HandlerFunc(api.ListBuckets)
    r.Methods("PUT").Path("/{bucket}").HandlerFunc(api.CreateBucket)
    r.Methods("DELETE").Path("/{bucket}").HandlerFunc(api.DeleteBucket)
    r.Methods("HEAD").Path("/{bucket}").HandlerFunc(api.HeadBucket)
    r.Methods("GET").Path("/{bucket}").HandlerFunc(api.ListObjects)

    // Multipart 操作
    r.Methods("POST").Path("/{bucket}/{object:.+}").Queries("uploads", "").
        HandlerFunc(api.CreateMultipartUpload)
    r.Methods("PUT").Path("/{bucket}/{object:.+}").
        Queries("partNumber", "{partNumber}", "uploadId", "{uploadId}").
        HandlerFunc(api.UploadPart)
    // ...

    // Object 操作
    r.Methods("PUT").Path("/{bucket}/{object:.+}").HandlerFunc(api.PutObject)
    r.Methods("GET").Path("/{bucket}/{object:.+}").HandlerFunc(api.GetObject)
    r.Methods("HEAD").Path("/{bucket}/{object:.+}").HandlerFunc(api.HeadObject)
    r.Methods("DELETE").Path("/{bucket}/{object:.+}").HandlerFunc(api.DeleteObject)

    // ...
}
```

注意一个细节: Multipart 路由必须放在 Object 路由前面,因为 `mux` 的路由匹配是按注册顺序来的。`{object:.+}` 这个正则会贪婪匹配路径,如果 Object 路由在前面,`POST /bucket/key?uploads` 就会被当成普通的 Object 操作处理。

### Bucket 操作

| 操作 | HTTP 方法 | 路径 | 说明 |
|------|-----------|------|------|
| CreateBucket | PUT | /{bucket} | 创建 Bucket,成功返回 200 + Location 头 |
| ListBuckets | GET | / | 列出所有 Bucket |
| DeleteBucket | DELETE | /{bucket} | 删除 Bucket,成功返回 204 |
| HeadBucket | HEAD | /{bucket} | 检查 Bucket 是否存在 |
| ListObjects | GET | /{bucket} | 列出 Bucket 中的对象 (ListObjectsV2 接口) |

### Object 操作

| 操作 | HTTP 方法 | 路径 | 说明 |
|------|-----------|------|------|
| PutObject | PUT | /{bucket}/{object} | 上传对象,返回 ETag |
| GetObject | GET | /{bucket}/{object} | 下载对象,支持 Range 请求 |
| DeleteObject | DELETE | /{bucket}/{object} | 删除对象,成功返回 204 |
| HeadObject | HEAD | /{bucket}/{object} | 获取对象元信息 |

### Multipart 操作

| 操作 | HTTP 方法 | 路径 | 说明 |
|------|-----------|------|------|
| CreateMultipartUpload | POST | /{bucket}/{object}?uploads | 初始化分片上传,返回 UploadId |
| UploadPart | PUT | /{bucket}/{object}?partNumber={n}&uploadId={id} | 上传分片,返回 ETag |
| CompleteMultipartUpload | POST | /{bucket}/{object}?uploadId={id} | 完成分片上传 |
| AbortMultipartUpload | DELETE | /{bucket}/{object}?uploadId={id} | 中止分片上传 |

## 1.5 响应格式

S3 API 用 XML 格式返回响应。mini-minio 通过 `writeXML` 和 `writeError` 两个辅助函数统一处理:

```go
func writeXML(w http.ResponseWriter, status int, v any) {
    w.Header().Set("Content-Type", "application/xml")
    w.WriteHeader(status)
    w.Write([]byte(xml.Header))
    xml.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
    type errResp struct {
        XMLName xml.Name `xml:"Error"`
        Code    string   `xml:"Code"`
        Message string   `xml:"Message"`
    }
    writeXML(w, status, errResp{Code: code, Message: message})
}
```

列出 Bucket 的响应:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult>
  <Buckets>
    <Bucket>
      <Name>my-bucket</Name>
      <CreationDate>2024-01-01T00:00:00Z</CreationDate>
    </Bucket>
  </Buckets>
</ListAllMyBucketsResult>
```

错误响应:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>NoSuchBucket</Code>
  <Message>bucket not found</Message>
</Error>
```

ListObjects 的响应比较复杂,包含了对象列表和公共前缀:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <Name>my-bucket</Name>
  <Prefix>photos/</Prefix>
  <KeyCount>2</KeyCount>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>
  <Contents>
    <Key>photos/2024/01/image.jpg</Key>
    <LastModified>2024-01-01T00:00:00Z</LastModified>
    <ETag>"d41d8cd98f00b204e9800998ecf8427e"</ETag>
    <Size>1024</Size>
  </Contents>
  <CommonPrefixes>
    <Prefix>photos/2024/02/</Prefix>
  </CommonPrefixes>
</ListBucketResult>
```

## 1.6 ObjectLayer: 核心抽象接口

整个 mini-minio 的架构围绕 `ObjectLayer` 接口展开。HTTP handler 只负责解析请求和组装响应,所有存储逻辑都通过这个接口:

```go
// object-api-interface.go
type ObjectLayer interface {
    MakeBucket(ctx context.Context, bucket string) error
    GetBucketInfo(ctx context.Context, bucket string) (bucketInfo BucketInfo, err error)
    ListBuckets(ctx context.Context) (buckets []BucketInfo, err error)
    DeleteBucket(ctx context.Context, bucket string) error

    ListObjectsV2(ctx context.Context, bucket, prefix, continuationToken, delimiter string,
        maxKeys int, startAfter string) (result ListObjectsV2Info, err error)
    GetObjectNInfo(ctx context.Context, bucket, object string, rs *HTTPRangeSpec) (reader *GetObjectReader, err error)
    GetObjectInfo(ctx context.Context, bucket, object string) (objInfo ObjectInfo, err error)
    PutObject(ctx context.Context, bucket, object string, data *PutObjReader) (objInfo ObjectInfo, err error)
    DeleteObject(ctx context.Context, bucket, object string) (ObjectInfo, error)
}
```

这个接口有两层实现:
- `erasureSets`: 最外层,负责把对象路由到正确的 erasure set
- `erasureObjects`: 每个 set 内部,负责实际的纠删码编码/解码和磁盘读写

对象路由用的是 CRC32 哈希:

```go
// erasure-sets.go
func (s *erasureSets) setForObject(object string) *erasureObjects {
    index := int(crc32.ChecksumIEEE([]byte(object)) % uint32(len(s.sets)))
    return s.sets[index]
}
```

## 1.7 Multipart Upload 的实现

mini-minio 的 Multipart Upload 用的是内存存储,所有分片数据都存在一个全局的 map 里:

```go
// erasure-object.go
var (
    multipartMu      sync.Mutex
    multipartUploads = map[string]*multipartUpload{}
)

type multipartUpload struct {
    bucket string
    object string
    id     string
    parts  map[int][]byte   // partNumber -> 数据
    etags  map[int]string   // partNumber -> ETag
    mu     sync.Mutex
}
```

CompleteMultipartUpload 时,把所有分片按顺序拼成一个完整的 io.Reader,然后调用 `PutObject` 走正常的纠删码写入流程:

```go
func completeMultipartUpload(ctx context.Context, ol ObjectLayer, uploadID string, partNumbers []int) (ObjectInfo, error) {
    // ... 从 map 中取出所有分片 ...
    var buf bytes.Buffer
    for _, n := range partNumbers {
        buf.Write(up.parts[n])
    }
    // 包装成 PutObjReader,调用 PutObject
    r, _ := NewPutObjReader(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
    return ol.PutObject(ctx, up.bucket, up.object, r)
}
```

这意味着如果上传一个很大的文件(比如几 GB),所有分片都会堆在内存里。原版 MinIO 的做法是把分片直接写到磁盘上,CompleteMultipartUpload 的时候只是组装元数据,不需要拷贝数据。不过作为学习项目,内存存储更容易理解。

## 1.8 原版 MinIO 与 mini-minio 的区别

### API 覆盖范围

原版 MinIO 实现了完整的 S3 API,包括版本控制、对象锁定、生命周期管理、事件通知、复制等等。mini-minio 只保留了最核心的部分:Bucket 的增删查、Object 的读写删、Multipart Upload 的基本流程、以及 Presigned URL。

### 认证机制

原版 MinIO 支持 SigV4、SigV2 (兼容旧客户端)、STS、IAM、LDAP/AD、OpenID Connect 等多种认证方式。mini-minio 只实现了 SigV4,而且是单用户模式 -- 只有一对 AccessKey/SecretKey,存在 `Credentials` 结构体里:

```go
type Credentials struct {
    AccessKey string
    SecretKey string
}
```

### 路由和中间件

原版 MinIO 用自定义路由器,有完整的中间件链(审计日志、请求追踪、限流等)。mini-minio 用 `gorilla/mux`,中间件只有一个认证层。

### 存储层

原版 MinIO 的存储层有很多高级特性:格式化磁盘管理、磁盘重连、自动修复 (healing)、数据均衡 (rebalance)、指标收集等。mini-minio 的 `erasureSets` 只保留了最核心的分组和路由逻辑。
