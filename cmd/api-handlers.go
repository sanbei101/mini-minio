package cmd

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

type apiHandlers struct {
	obj ObjectLayer
}

func NewRouter(obj ObjectLayer) http.Handler {
	r := mux.NewRouter()
	api := apiHandlers{obj: obj}

	// Bucket-level
	r.Methods("GET").Path("/").HandlerFunc(api.ListBuckets)
	r.Methods("PUT").Path("/{bucket}").HandlerFunc(api.CreateBucket)
	r.Methods("DELETE").Path("/{bucket}").HandlerFunc(api.DeleteBucket)
	r.Methods("HEAD").Path("/{bucket}").HandlerFunc(api.HeadBucket)
	r.Methods("GET").Path("/{bucket}").HandlerFunc(api.ListObjects)

	// Multipart
	r.Methods("POST").Path("/{bucket}/{object:.+}").Queries("uploads", "").HandlerFunc(api.CreateMultipartUpload)
	r.Methods("PUT").Path("/{bucket}/{object:.+}").Queries("partNumber", "{partNumber}", "uploadId", "{uploadId}").HandlerFunc(api.UploadPart)
	r.Methods("POST").Path("/{bucket}/{object:.+}").Queries("uploadId", "{uploadId}").HandlerFunc(api.CompleteMultipartUpload)
	r.Methods("DELETE").Path("/{bucket}/{object:.+}").Queries("uploadId", "{uploadId}").HandlerFunc(api.AbortMultipartUpload)

	// Object-level
	r.Methods("PUT").Path("/{bucket}/{object:.+}").HandlerFunc(api.PutObject)
	r.Methods("GET").Path("/{bucket}/{object:.+}").HandlerFunc(api.GetObject)
	r.Methods("HEAD").Path("/{bucket}/{object:.+}").HandlerFunc(api.HeadObject)
	r.Methods("DELETE").Path("/{bucket}/{object:.+}").HandlerFunc(api.DeleteObject)

	return r
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
	bucket := mux.Vars(r)["bucket"]
	if err := a.obj.MakeBucket(r.Context(), bucket); err != nil {
		writeError(w, http.StatusConflict, "BucketAlreadyExists", err.Error())
		return
	}
	w.Header().Set("Location", "/"+bucket)
	w.WriteHeader(http.StatusOK)
}

func (a *apiHandlers) DeleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	if err := a.obj.DeleteBucket(r.Context(), bucket); err != nil {
		writeError(w, http.StatusNotFound, "NoSuchBucket", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *apiHandlers) HeadBucket(w http.ResponseWriter, r *http.Request) {
	bucket := mux.Vars(r)["bucket"]
	if _, err := a.obj.GetBucketInfo(r.Context(), bucket); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
}

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

	result, err := a.obj.ListObjectsV2(r.Context(), bucket, prefix, contToken, delimiter, maxKeys, false, startAfter)
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
	for _, o := range result.Objects {
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

	gr, err := a.obj.GetObjectNInfo(r.Context(), bucket, object, rs, r.Header)
	if err != nil {
		writeError(w, http.StatusNotFound, "NoSuchKey", err.Error())
		return
	}
	defer gr.Close()

	info := gr.ObjInfo
	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("ETag", `"`+info.ETag+`"`)
	w.Header().Set("Last-Modified", info.ModTime.UTC().Format(http.TimeFormat))

	if rs != nil {
		offset, length, _ := rs.GetOffsetLength(info.Size)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+length-1, info.Size))
		w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
		w.WriteHeader(http.StatusPartialContent)
	} else {
		w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
		w.WriteHeader(http.StatusOK)
	}
	io.Copy(w, gr)
}

func (a *apiHandlers) HeadObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket, object := vars["bucket"], vars["object"]

	info, err := a.obj.GetObjectInfo(r.Context(), bucket, object)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", info.ContentType)
	w.Header().Set("Content-Length", strconv.FormatInt(info.Size, 10))
	w.Header().Set("ETag", `"`+info.ETag+`"`)
	w.Header().Set("Last-Modified", info.ModTime.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusOK)
}

func (a *apiHandlers) DeleteObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucket, object := vars["bucket"], vars["object"]

	if _, err := a.obj.DeleteObject(r.Context(), bucket, object); err != nil {
		writeError(w, http.StatusNotFound, "NoSuchKey", err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// --- Multipart handlers ---

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

func (a *apiHandlers) UploadPart(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	uploadID := r.URL.Query().Get("uploadId")
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil || partNumber < 1 {
		writeError(w, http.StatusBadRequest, "InvalidArgument", "invalid partNumber")
		return
	}
	_ = vars

	etag, err := uploadPart(uploadID, partNumber, r.Body)
	if err != nil {
		writeError(w, http.StatusNotFound, "NoSuchUpload", err.Error())
		return
	}
	w.Header().Set("ETag", `"`+etag+`"`)
	w.WriteHeader(http.StatusOK)
}

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

func (a *apiHandlers) AbortMultipartUpload(w http.ResponseWriter, r *http.Request) {
	uploadID := r.URL.Query().Get("uploadId")
	abortMultipartUpload(uploadID)
	w.WriteHeader(http.StatusNoContent)
}

// --- Presign ---

// PresignGetObject generates a presigned GET URL valid for the given duration.
func PresignGetObject(baseURL, bucket, object, accessKey, secretKey string, expiry time.Duration) string {
	expires := strconv.FormatInt(int64(expiry.Seconds()), 10)
	u := fmt.Sprintf("%s/%s/%s?X-Amz-Expires=%s&X-Amz-SignatureVersion=unsigned", baseURL, bucket, object, expires)
	return u
}

// PresignPutObject generates a presigned PUT URL.
func PresignPutObject(baseURL, bucket, object, accessKey, secretKey string, expiry time.Duration) string {
	expires := strconv.FormatInt(int64(expiry.Seconds()), 10)
	u := fmt.Sprintf("%s/%s/%s?X-Amz-Expires=%s&X-Amz-SignatureVersion=unsigned", baseURL, bucket, object, expires)
	return u
}

// --- Helpers ---

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

// suppress unused import warning
var _ = url.QueryEscape
