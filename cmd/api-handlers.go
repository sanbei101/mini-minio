package cmd

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"strconv"
	"strings"
	"time"

	"github.com/phuslu/log"
)

type apiHandlers struct {
	obj   ObjectLayer
	creds Credentials
}

func NewRouter(obj ObjectLayer, creds Credentials) http.Handler {
	mux := http.NewServeMux()
	api := &apiHandlers{obj: obj, creds: creds}

	// Bucket-level
	mux.HandleFunc("GET /{$}", api.ListBuckets)
	mux.HandleFunc("PUT /{bucket}/{$}", api.CreateBucket)
	mux.HandleFunc("DELETE /{bucket}/{$}", api.DeleteBucket)
	mux.HandleFunc("HEAD /{bucket}/{$}", api.HeadBucket)
	mux.HandleFunc("GET /{bucket}/{$}", api.ListObjects)

	// Multipart and Object level
	mux.HandleFunc("PUT /{bucket}/{object...}", api.dispatchPut)
	mux.HandleFunc("POST /{bucket}/{object...}", api.dispatchPost)
	mux.HandleFunc("DELETE /{bucket}/{object...}", api.dispatchDelete)
	mux.HandleFunc("GET /{bucket}/{object...}", api.dispatchGet)

	if creds.AccessKey == "" {
		return mux
	}
	return requestLoggingMiddleware(authMiddleware(creds, mux))
}

func (a *apiHandlers) dispatchPut(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Has("uploadId") && q.Has("partNumber") {
		a.UploadPart(w, r)
		return
	}
	a.PutObject(w, r)
}

func (a *apiHandlers) dispatchPost(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Has("uploads") {
		a.CreateMultipartUpload(w, r)
		return
	}
	if q.Has("uploadId") {
		a.CompleteMultipartUpload(w, r)
		return
	}
	writeError(w, http.StatusBadRequest, "InvalidRequest", "unsupported POST operation")
}

func (a *apiHandlers) dispatchDelete(w http.ResponseWriter, r *http.Request) {
	if r.URL.Query().Has("uploadId") {
		a.AbortMultipartUpload(w, r)
		return
	}
	a.DeleteObject(w, r)
}

func (a *apiHandlers) dispatchGet(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodHead {
		a.HeadObject(w, r)
		return
	}
	a.GetObject(w, r)
}

type loggingResponseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (lrw *loggingResponseWriter) WriteHeader(code int) {
	lrw.statusCode = code
	lrw.ResponseWriter.WriteHeader(code)
}

func requestLoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lrw := &loggingResponseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		start := time.Now()

		next.ServeHTTP(lrw, r)
		if lrw.statusCode < 200 || lrw.statusCode >= 300 {
			log.Info().
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("query", r.URL.RawQuery).
				Int("status", lrw.statusCode).
				Dur("duration", time.Since(start)).
				Msg("http request")
		}
	})
}

func authMiddleware(creds Credentials, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		switch {
		case r.URL.Query().Get("X-Amz-Signature") != "":
			// Presigned request
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

// --- Bucket handlers ---

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

func (a *apiHandlers) CreateBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	if err := a.obj.MakeBucket(r.Context(), bucket); err != nil {
		writeError(w, http.StatusConflict, "BucketAlreadyExists", err.Error())
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (a *apiHandlers) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	if err := a.obj.DeleteBucket(r.Context(), bucket); err != nil {
		writeError(w, http.StatusNotFound, "NoSuchBucket", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *apiHandlers) HeadBucket(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
	if _, err := a.obj.GetBucketInfo(r.Context(), bucket); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (a *apiHandlers) ListObjects(w http.ResponseWriter, r *http.Request) {
	bucket := r.PathValue("bucket")
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

// --- Object handlers ---

func (a *apiHandlers) PutObject(w http.ResponseWriter, r *http.Request) {
	bucket, object := r.PathValue("bucket"), r.PathValue("object")

	var body io.Reader = r.Body
	size := r.ContentLength
	if strings.Contains(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") ||
		strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		body = httputil.NewChunkedReader(r.Body)
		if decodedLen := r.Header.Get("X-Amz-Decoded-Content-Length"); decodedLen != "" {
			if s, err := strconv.ParseInt(decodedLen, 10, 64); err == nil {
				size = s
			}
		}
	}
	reader, err := NewPutObjReader(body, size)
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

func (a *apiHandlers) GetObject(w http.ResponseWriter, r *http.Request) {
	bucket, object := r.PathValue("bucket"), r.PathValue("object")

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

func (a *apiHandlers) HeadObject(w http.ResponseWriter, r *http.Request) {
	bucket, object := r.PathValue("bucket"), r.PathValue("object")

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

func (a *apiHandlers) DeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket, object := r.PathValue("bucket"), r.PathValue("object")

	if _, err := a.obj.DeleteObject(r.Context(), bucket, object); err != nil {
		writeError(w, http.StatusNotFound, "NoSuchKey", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Multipart handlers ---

func (a *apiHandlers) CreateMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket, object := r.PathValue("bucket"), r.PathValue("object")
	uploadID, err := a.obj.NewMultipartUpload(r.Context(), bucket, object)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "InternalError", err.Error())
		return
	}

	type resp struct {
		XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
		Bucket   string   `xml:"Bucket"`
		Key      string   `xml:"Key"`
		UploadID string   `xml:"UploadId"`
	}
	writeXML(w, http.StatusOK, resp{Bucket: bucket, Key: object, UploadID: uploadID})
}

func (a *apiHandlers) UploadPart(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("uploadId")
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || partNumber < 1 {
		writeError(w, http.StatusBadRequest, "InvalidArgument", "invalid partNumber")
		return
	}

	var body io.Reader = r.Body
	size := r.ContentLength
	if strings.Contains(r.Header.Get("X-Amz-Content-Sha256"), "STREAMING-") ||
		strings.Contains(r.Header.Get("Content-Encoding"), "aws-chunked") {
		body = httputil.NewChunkedReader(r.Body)
		if decodedLen := r.Header.Get("X-Amz-Decoded-Content-Length"); decodedLen != "" {
			if parsed, parseErr := strconv.ParseInt(decodedLen, 10, 64); parseErr == nil {
				size = parsed
			}
		}
	}
	reader, err := NewPutObjReader(body, size)
	if err != nil {
		writeError(w, http.StatusBadRequest, "InvalidRequest", err.Error())
		return
	}
	part, err := a.obj.PutObjectPart(
		r.Context(),
		r.PathValue("bucket"),
		r.PathValue("object"),
		uploadID,
		partNumber,
		reader,
	)
	if err != nil {
		writeError(w, http.StatusNotFound, "NoSuchUpload", err.Error())
		return
	}
	w.Header().Set("ETag", `"`+part.ETag+`"`)
	w.WriteHeader(http.StatusOK)
}

func (a *apiHandlers) CompleteMultipartUpload(w http.ResponseWriter, r *http.Request) {
	bucket, object := r.PathValue("bucket"), r.PathValue("object")
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

	info, err := a.obj.CompleteMultipartUpload(r.Context(), bucket, object, uploadID, partNumbers)
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

func (a *apiHandlers) AbortMultipartUpload(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("uploadId")
	if err := a.obj.AbortMultipartUpload(
		r.Context(),
		r.PathValue("bucket"),
		r.PathValue("object"),
		uploadID,
	); err != nil {
		writeError(w, http.StatusNotFound, "NoSuchUpload", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Presign ---

func PresignGetObject(baseURL, bucket, object, accessKey, secretKey string, expiry time.Duration) string {
	return PresignURL(baseURL, http.MethodGet, bucket, object, accessKey, secretKey, expiry)
}

func PresignPutObject(baseURL, bucket, object, accessKey, secretKey string, expiry time.Duration) string {
	return PresignURL(baseURL, http.MethodPut, bucket, object, accessKey, secretKey, expiry)
}

// --- Helpers ---
func writeXML(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	if _, err := w.Write([]byte(xml.Header)); err != nil {
		log.Error().Err(err).Msg("Failed to write XML header")
		return
	}
	if err := xml.NewEncoder(w).Encode(v); err != nil {
		log.Error().Err(err).Msg("Failed to write XML response")
		return
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	type errResp struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}
	writeXML(w, status, errResp{Code: code, Message: message})
}

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
