# 第5章: 衍生出的二级接口

## 5.1 Multipart Upload (分片上传)

大文件上传是个老大难的问题——网络抖动一下,前面传的就全白费了。分片上传就是为了解决这个痛点:把大文件拆成小块,一块一块传,最后再合起来。

mini-minio 的分片上传用的是纯内存方案。所有分片数据存在 `map` 里,完成时拼到一起调 `PutObject` 写盘。这个方案简单粗暴,但对学习来说够用了。

### 数据结构

```go
// cmd/erasure-object.go:656
type multipartUpload struct {
    bucket string
    object string
    id     string
    parts  map[int][]byte    // partNumber -> 分片数据
    etags  map[int]string    // partNumber -> 分片 MD5
    mu     sync.Mutex
}

var (
    multipartMu      sync.Mutex
    multipartUploads = map[string]*multipartUpload{}
)
```

两个 `map` 用 `sync.Mutex` 保护。外层的 `multipartMu` 保护全局的 `multipartUploads` map,内层的 `mu` 保护单次上传的 `parts` 和 `etags`。这个双层锁的设计粒度还算合理——查找上传用全局锁,读写分片数据用单次上传的锁。

### CreateMultipartUpload

客户端发起分片上传的第一步。服务端生成一个 UUID 作为 uploadID,把这个上传记到内存 map 里,然后把 uploadID 返给客户端。后续所有 `UploadPart`、`CompleteMultipartUpload`、`AbortMultipartUpload` 请求都靠这个 uploadID 来定位是哪次上传。

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

对应的底层实现:

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

`uuid.New()` 保证了 uploadID 的唯一性。整个过程没有持久化,服务重启后 uploadID 就丢了。

### UploadPart

客户端拿到 uploadID 后,就可以开始上传分片了。每个分片带一个 `partNumber`(从 1 开始),服务端把整个分片读进内存,算个 MD5 作为 ETag,然后存到 `parts` map 里。

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

注意 `partNumber` 必须大于等于 1,否则直接返回 400。uploadID 找不到则返回 404。ETag 返回时要加上双引号,这是 S3 规范要求的。

底层实现就是 `io.ReadAll` 把整个分片吃进内存:

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

这里有个明显的局限:分片大小没有上限。客户端传一个 2GB 的"分片"进来,服务端也会老老实实读完,然后内存直接爆掉。

### CompleteMultipartUpload

所有分片上传完毕后,客户端发一个 `CompleteMultipartUpload` 请求,带上每个分片的 partNumber 和 ETag。服务端按 partNumber 顺序把分片拼起来,然后调 `PutObject` 写入纠删码存储。

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

注意这里返回的 ETag 是整个对象的 MD5(由 `PutObject` 计算),不是各个分片的 MD5。S3 规范里分片上传的 ETag 格式是 `{md5}-{partCount}`,但 mini-minio 为了简化没有做这个处理。

底层的 `completeMultipartUpload` 做的事情很直白:按顺序拼接分片,然后扔给 `PutObject`:

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

拼接用的是 `bytes.Buffer`——所有分片数据会在内存里再复制一份。如果一个 1GB 的文件分了 100 个 10MB 的片,这里会临时占用大约 2GB 内存(分片数据 + Buffer 拼接)。写入成功后,立即从 map 中删除这次上传,释放内存。

### AbortMultipartUpload

客户端可以随时放弃一次分片上传。服务端做的事情很简单:从 map 里删掉就行了。

```go
// cmd/api-handlers.go:387
func (a *apiHandlers) AbortMultipartUpload(w http.ResponseWriter, r *http.Request) {
    uploadID := r.URL.Query().Get("uploadId")
    abortMultipartUpload(uploadID)
    w.WriteHeader(http.StatusNoContent)
}
```

```go
// cmd/erasure-object.go:746
func abortMultipartUpload(uploadID string) {
    multipartMu.Lock()
    delete(multipartUploads, uploadID)
    multipartMu.Unlock()
}
```

注意这里不管 uploadID 是否存在都会返回 204。这是符合 S3 规范的——Abort 本身就是幂等的,不存在就算成功了。

### 与原版的差距

这个内存方案有几个明显的问题:

| 特性 | 原版 MinIO | mini-minio |
|------|-----------|------------|
| 存储位置 | 磁盘(`.minio.sys/multipart/...`) | 内存 |
| 纠删码 | 每个分片独立编码存储 | 最终合并后统一编码 |
| 位腐保护 | 每个分片有校验和 | 无 |
| ETag 验证 | 完成时逐片验证 | 不验证 |
| 持久化 | 支持,重启不丢失 | 重启全丢 |
| 过期清理 | 自动清理过期上传 | 不清理 |
| 列表支持 | ListMultipartUploads, ListObjectParts | 不支持 |
| 内存使用 | 低(流式写盘) | 高(全在内存) |

原版 MinIO 的分片上传是这样的:每个分片独立做纠删码编码,写到磁盘上;完成时不是拼接,而是构建一个 `xl.meta` 指向所有分片,然后原子重命名。这样既避免了内存爆炸,又保证了崩溃安全。

## 5.2 Presigned URL (预签名 URL)

预签名 URL 解决的是一个常见需求:我想让浏览器直接往 S3 传文件,但又不想把 AccessKey/SecretKey 暴露给前端。答案就是服务端用密钥签一个有时效的 URL 给前端,前端拿着这个 URL 就能直接操作对象。

mini-minio 的预签名实现完全遵循 AWS Signature V4 规范。下面拆开来讲。

### 生成预签名 URL

`PresignURL` 函数在 `cmd/signature-v4.go` 里,接受 baseURL、HTTP 方法、bucket、object、密钥和过期时间,返回一个完整的带签名的 URL。

```go
// cmd/signature-v4.go:224
func PresignURL(baseURL, method, bucket, object, accessKey, secretKey string, expiry time.Duration) string {
    t := time.Now().UTC()
    host := strings.TrimPrefix(strings.TrimPrefix(baseURL, "https://"), "http://")

    q := url.Values{}
    q.Set("X-Amz-Algorithm", signV4Algorithm)
    q.Set("X-Amz-Credential", accessKey+"/"+scope(t))
    q.Set("X-Amz-Date", t.Format(iso8601Format))
    q.Set("X-Amz-Expires", strconv.Itoa(int(expiry.Seconds())))
    q.Set("X-Amz-SignedHeaders", "host")

    path := "/" + bucket + "/" + object
    canonHdr := "host:" + host + "\n"
    signedStr := "host"

    canonReq := strings.Join([]string{
        method,
        path,
        canonicalQueryString(q, false),
        canonHdr,
        signedStr,
        unsignedPayload,
    }, "\n")

    stringToSign := signV4Algorithm + "\n" +
        t.Format(iso8601Format) + "\n" +
        scope(t) + "\n" +
        hashSHA256([]byte(canonReq))

    key := signingKey(secretKey, t)
    sig := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))
    q.Set("X-Amz-Signature", sig)

    return baseURL + path + "?" + q.Encode()
}
```

这个函数做的事情按顺序拆解:

1. 从 `baseURL` 里提取 host(注意不是用 `url.Parse`,而是简单地 `TrimPrefix` 去掉协议头)
2. 构造查询参数:算法、凭证范围、日期、过期时间、签名头列表
3. 拼接规范请求(canonical request):方法、路径、排序后的查询参数、规范头部、签名头列表、payload hash
4. 构造待签名字符串(string to sign):算法 + 日期 + 凭证范围 + 规范请求的 SHA256
5. 派生签名密钥,计算 HMAC-SHA256 签名
6. 把签名附加到查询参数里,返回完整 URL

注意这里 payload hash 固定用 `unsignedPayload`(即字符串 `"UNSIGNED-PAYLOAD"`),因为预签名 URL 生成的时候还不知道请求体内容。

下面两个便捷函数是对 `PresignURL` 的简单包装:

```go
// cmd/api-handlers.go:395
func PresignGetObject(baseURL, bucket, object, accessKey, secretKey string, expiry time.Duration) string {
    return PresignURL(baseURL, http.MethodGet, bucket, object, accessKey, secretKey, expiry)
}

func PresignPutObject(baseURL, bucket, object, accessKey, secretKey string, expiry time.Duration) string {
    return PresignURL(baseURL, http.MethodPut, bucket, object, accessKey, secretKey, expiry)
}
```

### 验证预签名 URL

当客户端用预签名 URL 发请求时,服务端需要验证签名是否合法、是否过期。验证逻辑在 `verifyPresignedAuth` 里:

```go
// cmd/signature-v4.go:165
func verifyPresignedAuth(r *http.Request, creds Credentials) error {
    q := r.URL.Query()

    if q.Get("X-Amz-Algorithm") != signV4Algorithm {
        return errors.New("unsupported algorithm")
    }
    credStr := q.Get("X-Amz-Credential")
    credParts := strings.Split(credStr, "/")
    if len(credParts) < 5 || credParts[0] != creds.AccessKey {
        return errors.New("unknown access key")
    }
    t, err := time.Parse(iso8601Format, q.Get("X-Amz-Date"))
    if err != nil {
        return errors.New("invalid X-Amz-Date")
    }
    expires, err := time.ParseDuration(q.Get("X-Amz-Expires") + "s")
    if err != nil {
        return errors.New("invalid X-Amz-Expires")
    }
    if time.Since(t) > expires {
        return errors.New("presigned URL expired")
    }

    signedHeaders := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
    hdr := make(http.Header)
    for _, k := range signedHeaders {
        if k == "host" {
            hdr.Set("Host", r.Host)
        } else {
            hdr[http.CanonicalHeaderKey(k)] = r.Header[http.CanonicalHeaderKey(k)]
        }
    }
    canonHdr, signedStr := canonicalHeaders(hdr, signedHeaders)

    canonReq := strings.Join([]string{
        r.Method,
        r.URL.EscapedPath(),
        canonicalQueryString(q, true), // exclude X-Amz-Signature
        canonHdr,
        signedStr,
        unsignedPayload,
    }, "\n")

    stringToSign := signV4Algorithm + "\n" +
        t.Format(iso8601Format) + "\n" +
        scope(t) + "\n" +
        hashSHA256([]byte(canonReq))

    key := signingKey(creds.SecretKey, t)
    expected := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))
    signature := q.Get("X-Amz-Signature")

    if subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) != 1 {
        return errors.New("signature mismatch")
    }
    return nil
}
```

几个值得留意的细节:

- **过期检查**: `X-Amz-Expires` 的值是秒数,代码用 `time.ParseDuration(value + "s")` 拼上 "s" 后缀转成 `time.Duration`,然后跟 `time.Since(t)` 比较。超过有效期直接拒绝。
- **查询参数排序**: 调用 `canonicalQueryString(q, true)` 时第二个参数 `true` 表示排除 `X-Amz-Signature` 本身——签名计算当然不能把自己也算进去。
- **常量时间比较**: 用 `subtle.ConstantTimeCompare` 而不是 `==` 来比对签名,防止时序攻击。
- **host 头处理**: `host` 不在 `r.Header` 里(它是 HTTP/1.1 的特殊头),所以需要从 `r.Host` 单独取。

### 签名算法的核心组件

签名相关的辅助函数都集中在 `cmd/signature-v4.go` 里:

**签名密钥派生** (`signingKey`):AWS 的签名密钥不是直接用 SecretKey,而是经过四层 HMAC-SHA256 派生:

```go
// cmd/signature-v4.go:44
func signingKey(secretKey string, t time.Time) []byte {
    date := hmacSHA256([]byte("AWS4"+secretKey), []byte(t.Format(yyyymmdd)))
    reg := hmacSHA256(date, []byte(region))
    svc := hmacSHA256(reg, []byte(service))
    return hmacSHA256(svc, []byte("aws4_request"))
}
```

派生链: `SecretKey` -> `date` -> `region` -> `service` -> `aws4_request`。每一层都把上一层的输出作为下一层的 key。

**凭证范围** (`scope`):格式是 `{date}/{region}/{service}/aws4_request`。mini-minio 硬编码了 `us-east-1` 和 `s3`。

```go
// cmd/signature-v4.go:51
func scope(t time.Time) string {
    return t.Format(yyyymmdd) + "/" + region + "/" + service + "/aws4_request"
}
```

**查询参数规范化** (`canonicalQueryString`):按参数名字母排序,URL 编码后用 `&` 连接。`excludeSig` 参数用于预签名场景,计算签名时要排除 `X-Amz-Signature` 自身:

```go
// cmd/signature-v4.go:57
func canonicalQueryString(q url.Values, excludeSig bool) string {
    keys := make([]string, 0, len(q))
    for k := range q {
        if excludeSig && k == "X-Amz-Signature" {
            continue
        }
        keys = append(keys, k)
    }
    sort.Strings(keys)
    var parts []string
    for _, k := range keys {
        for _, v := range q[k] {
            parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(v))
        }
    }
    return strings.Join(parts, "&")
}
```

**头部规范化** (`canonicalHeaders`):把指定的头部名字转小写,值去掉前后空白,然后按名字排序拼接。返回值有两个:规范头部字符串和签名头列表(分号分隔):

```go
// cmd/signature-v4.go:75
func canonicalHeaders(h http.Header, signed []string) (canonical, signedStr string) {
    m := make(map[string]string, len(signed))
    for _, k := range signed {
        lk := strings.ToLower(k)
        m[lk] = strings.TrimSpace(h.Get(k))
    }
    // host is special — not in r.Header
    keys := make([]string, 0, len(m))
    for k := range m {
        keys = append(keys, k)
    }
    sort.Strings(keys)
    var sb strings.Builder
    for _, k := range keys {
        sb.WriteString(k)
        sb.WriteByte(':')
        sb.WriteString(m[k])
        sb.WriteByte('\n')
    }
    return sb.String(), strings.Join(keys, ";")
}
```

注释里提到 `host is special — not in r.Header`——Go 的 `http.Header.Get("Host")` 拿不到值,所以调用方需要把 `r.Host` 手动塞进去。

### Authorization Header 认证

除了预签名 URL,mini-minio 还支持标准的 `Authorization` Header 认证。验证逻辑在 `verifyHeaderAuth` 里:

```go
// cmd/signature-v4.go:98
func verifyHeaderAuth(r *http.Request, creds Credentials) error {
    auth := r.Header.Get("Authorization")
    if !strings.HasPrefix(auth, signV4Algorithm+" ") {
        return errors.New("missing or unsupported Authorization header")
    }
    rest := strings.TrimPrefix(auth, signV4Algorithm+" ")
    credStr := extractAuthField(rest, "Credential")
    signedHeadersStr := extractAuthField(rest, "SignedHeaders")
    signature := extractAuthField(rest, "Signature")
    if credStr == "" || signedHeadersStr == "" || signature == "" {
        return errors.New("malformed Authorization header")
    }
    // ... 验证签名
}
```

Authorization 头的格式是 `AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...`。解析用的是 `extractAuthField`:

```go
// cmd/signature-v4.go:262
func extractAuthField(s, field string) string {
    prefix := field + "="
    for _, part := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' }) {
        part = strings.TrimSpace(part)
        if after, ok := strings.CutPrefix(part, prefix); ok {
            return after
        }
    }
    return ""
}
```

这个函数按逗号分割,然后逐段查找目标字段。用 `strings.CutPrefix` 而不是 `strings.TrimPrefix`,因为 `CutPrefix` 返回 `ok` 布尔值,能区分"找到了空前缀"和"没找到"的情况。

Header 认证和预签名认证的核心区别在于:Header 认证的 payload hash 来自 `X-Amz-Content-Sha256` 头(如果没设就用 `unsignedPayload`),而预签名认证固定用 `unsignedPayload`。

### 认证中间件

认证中间件是整个认证流程的入口:

```go
// cmd/api-handlers.go:61
func authMiddleware(creds Credentials, next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        var err error
        switch {
        case r.URL.Query().Get("X-Amz-Signature") != "":
            if err = r.ParseForm(); err == nil {
                err = verifyPresignedAuth(r, creds)
            }
        case r.Header.Get("Authorization") != "":
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

逻辑很直接:先看查询参数里有没有 `X-Amz-Signature`(预签名),再看有没有 `Authorization` 头,都没有就报错。预签名请求需要先 `ParseForm` 把查询参数解析到 `r.Form` 里。

路由注册时,如果没配置凭证(`AccessKey == ""`),就跳过认证中间件:

```go
// cmd/api-handlers.go:22
func NewRouter(obj ObjectLayer, creds Credentials) http.Handler {
    r := mux.NewRouter()
    api := apiHandlers{obj: obj, creds: creds}

    // Bucket-level
    r.Methods("GET").Path("/").HandlerFunc(api.ListBuckets)
    r.Methods("PUT").Path("/{bucket}").HandlerFunc(api.CreateBucket)
    r.Methods("DELETE").Path("/{bucket}").HandlerFunc(api.DeleteBucket)
    r.Methods("HEAD").Path("/{bucket}").HandlerFunc(api.HeadBucket)
    r.Methods("GET").Path("/{bucket}").HandlerFunc(api.ListObjects)

    // Multipart
    r.Methods("POST").Path("/{bucket}/{object:.+}").Queries("uploads", "").HandlerFunc(api.CreateMultipartUpload)
    r.Methods("PUT").
        Path("/{bucket}/{object:.+}").
        Queries("partNumber", "{partNumber}", "uploadId", "{uploadId}").
        HandlerFunc(api.UploadPart)
    r.Methods("POST").
        Path("/{bucket}/{object:.+}").
        Queries("uploadId", "{uploadId}").
        HandlerFunc(api.CompleteMultipartUpload)
    r.Methods("DELETE").
        Path("/{bucket}/{object:.+}").
        Queries("uploadId", "{uploadId}").
        HandlerFunc(api.AbortMultipartUpload)

    // Object-level
    r.Methods("PUT").Path("/{bucket}/{object:.+}").HandlerFunc(api.PutObject)
    r.Methods("GET").Path("/{bucket}/{object:.+}").HandlerFunc(api.GetObject)
    r.Methods("HEAD").Path("/{bucket}/{object:.+}").HandlerFunc(api.HeadObject)
    r.Methods("DELETE").Path("/{bucket}/{object:.+}").HandlerFunc(api.DeleteObject)

    if creds.AccessKey == "" {
        return r
    }
    return authMiddleware(creds, r)
}
```

路由用的是 `gorilla/mux`。注意 multipart 相关的路由靠 `Queries` 条件区分——同一个 `POST /{bucket}/{object:.+}` 路径,带 `uploads` 参数的是 `CreateMultipartUpload`,带 `uploadId` 参数的是 `CompleteMultipartUpload`。

### 错误响应

所有错误都通过 `writeError` 返回 XML 格式的错误信息:

```go
// cmd/api-handlers.go:417
func writeError(w http.ResponseWriter, status int, code, message string) {
    type errResp struct {
        XMLName xml.Name `xml:"Error"`
        Code    string   `xml:"Code"`
        Message string   `xml:"Message"`
    }
    writeXML(w, status, errResp{Code: code, Message: message})
}
```

这和 S3 的错误响应格式一致:一个 `<Error>` 根元素,下面有 `<Code>` 和 `<Message>`。常见的错误码有 `AccessDenied`(403)、`NoSuchKey`(404)、`InvalidRange`(400) 等。

## 5.3 Range 下载支持

Range 下载允许客户端只下载对象的一部分。视频播放器拖进度条的时候,就是靠 Range 请求跳到指定位置的。

### Range 格式

S3 支持三种 Range 写法:

- `bytes=0-499` — 前 500 字节(闭区间,所以是 0 到 499)
- `bytes=500-` — 从第 500 字节到结尾
- `bytes=-500` — 最后 500 字节

### HTTPRangeSpec 结构

```go
// cmd/httprange.go
type HTTPRangeSpec struct {
    IsSuffixLength bool    // true 表示 bytes=-N 这种后缀写法
    Start, End     int64   // IsSuffixLength=true 时 Start 是负数(-N);否则 Start 是起始偏移,End 是结束偏移(-1 表示到结尾)
}
```

### 解析 Range 头

`parseRangeSpec` 在 `cmd/api-handlers.go` 里:

```go
// cmd/api-handlers.go:426
func parseRangeSpec(s string) (*HTTPRangeSpec, error) {
    s = strings.TrimPrefix(s, "bytes=")
    if strings.HasPrefix(s, "-") {
        n, err := strconv.ParseInt(s, 10, 64)
        if err != nil {
            return nil, err
        }
        return &HTTPRangeSpec{IsSuffixLength: true, Start: n}, nil
    }
    parts := strings.SplitN(s, "-", 2)
    if len(parts) != 2 {
        return nil, fmt.Errorf("invalid range: %s", s)
    }
    start, err := strconv.ParseInt(parts[0], 10, 64)
    if err != nil {
        return nil, err
    }
    end := int64(-1)
    if parts[1] != "" {
        end, err = strconv.ParseInt(parts[1], 10, 64)
        if err != nil {
            return nil, err
        }
    }
    return &HTTPRangeSpec{Start: start, End: end}, nil
}
```

先判断是不是后缀写法(以 `-` 开头),然后按 `-` 分割。`End` 为空(如 `bytes=500-`)时设为 -1,表示到文件末尾。

### 计算偏移量和长度

`GetOffsetLength` 把 Range 规范转成实际的 offset 和 length:

```go
// cmd/erasure-object.go:638
func (rs *HTTPRangeSpec) GetOffsetLength(size int64) (int64, int64, error) {
    if rs.IsSuffixLength {
        start := max(size+rs.Start, 0)  // rs.Start 是负数,所以 size+(-N) = size-N
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

后缀写法 `bytes=-500` 对应 `Start = -500`,所以 `size + (-500)` 就是倒数第 500 字节的位置。`max(..., 0)` 防止文件比请求的字节数还小的情况。

### HTTP 响应

在 `GetObject` handler 里,Range 请求返回 206 Partial Content:

```go
// cmd/api-handlers.go:250
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
```

`Content-Range` 的格式是 `bytes {start}-{end}/{total}`。注意 `Content-Length` 设置的是实际传输的字节数(length),不是对象的总大小。

Range 的实际解码在纠删码层完成——`GetObjectNInfo` 把 offset 和 length 传给 `enc.Decode`,纠删码引擎只解码需要的那部分数据,而不是把整个文件解码出来再截取。这对大文件的随机读很重要。

## 5.4 与原版 MinIO 的对比

### Multipart Upload

原版 MinIO 的分片上传是个完整的生产级方案:

1. **磁盘持久化**: 分片数据存在 `.minio.sys/multipart/bucket/sha256(bucket+object)/uploadID/` 目录下,每个分片独立做纠删码编码
2. **元数据跟踪**: `uploads.json` 记录所有分片信息,支持 `ListMultipartUploads` 和 `ListObjectParts`
3. **ETag 验证**: 完成时逐片校验 ETag,防止传输错误
4. **原子完成**: 完成时不拼接分片,而是构建 `xl.meta` 指向所有分片数据,然后原子重命名
5. **过期清理**: 后台任务自动清理过期的分片上传
6. **位腐保护**: 每个分片有校验和,读取时校验

mini-minio 的方案简单得多,但也有它的优势:代码量少,容易理解。如果只是学习纠删码和 S3 API 的核心概念,这个简化版足够了。

### Presigned URL

原版 MinIO 的预签名支持更全面:
- 支持 STS 临时凭证
- 支持多租户
- 支持所有 HTTP 方法(不只是 GET/PUT)
- 支持 postPolicy(浏览器表单上传)

mini-minio 只实现了 GET 和 PUT 的预签名,但覆盖了 SigV4 签名的核心流程。

### Range 下载

原版支持:
- 多 Range 请求(Multipart Range,一次请求下载多段)
- 条件请求(If-Match, If-None-Match, If-Modified-Since 等)
- Range 和条件请求的组合

mini-minio 只支持单个 Range 请求,不支持条件请求。但单 Range 已经覆盖了最常用的场景(视频拖进度、断点续传)。

### 认证

原版 MinIO 的认证体系非常庞大:SigV4、SigV2(兼容旧客户端)、STS、IAM、LDAP/AD、OpenID Connect、多租户、匿名访问、细粒度权限控制。

mini-minio 只实现了 SigV4 的 Header 认证和预签名认证,加上"无凭证时跳过认证"的匿名模式。对于学习目的来说,这就够了——核心的签名算法和验证流程都在。
