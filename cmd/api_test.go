package cmd_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/sanbei101/mini-minio/cmd"
)

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func hashSHA256(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func signingKey(secretKey string, t time.Time) []byte {
	date := hmacSHA256([]byte("AWS4"+secretKey), []byte(t.Format("20060102")))
	reg := hmacSHA256(date, []byte("us-east-1"))
	svc := hmacSHA256(reg, []byte("s3"))
	return hmacSHA256(svc, []byte("aws4_request"))
}

// signRequest adds a SigV4 Authorization header to req.
func signRequest(t *testing.T, req *http.Request, accessKey, secretKey string) {
	t.Helper()
	now := time.Now().UTC()
	dateStr := now.Format("20060102T150405Z")
	dateOnly := now.Format("20060102")
	scope := dateOnly + "/us-east-1/s3/aws4_request"

	req.Header.Set("X-Amz-Date", dateStr)
	req.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	if req.Host == "" {
		req.Host = req.URL.Host
	}

	signedHeaders := "host;x-amz-content-sha256;x-amz-date"
	canonHdr := fmt.Sprintf("host:%s\nx-amz-content-sha256:UNSIGNED-PAYLOAD\nx-amz-date:%s\n", req.Host, dateStr)

	q := req.URL.Query()
	qKeys := make([]string, 0, len(q))
	for k := range q {
		qKeys = append(qKeys, k)
	}
	sort.Strings(qKeys)
	var qParts []string
	for _, k := range qKeys {
		for _, v := range q[k] {
			qParts = append(qParts, url.QueryEscape(k)+"="+url.QueryEscape(v))
		}
	}
	canonQuery := strings.Join(qParts, "&")

	canonReq := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		canonQuery,
		canonHdr,
		signedHeaders,
		"UNSIGNED-PAYLOAD",
	}, "\n")

	stringToSign := "AWS4-HMAC-SHA256\n" + dateStr + "\n" + scope + "\n" + hashSHA256([]byte(canonReq))
	key := signingKey(secretKey, now)
	sig := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))

	req.Header.Set("Authorization", fmt.Sprintf(
		"AWS4-HMAC-SHA256 Credential=%s/%s,SignedHeaders=%s,Signature=%s",
		accessKey, scope, signedHeaders, sig,
	))
}

func setup(t *testing.T) (*httptest.Server, string, string) {
	t.Helper()
	disks := make([]string, 6)
	for i := range disks {
		disks[i] = t.TempDir()
	}
	obj, err := cmd.NewErasureObjects(disks, 4, 2)
	if err != nil {
		t.Fatal(err)
	}
	ak, sk := "testkey", "testsecret"
	srv := httptest.NewServer(cmd.NewRouter(obj, cmd.Credentials{AccessKey: ak, SecretKey: sk}))
	t.Cleanup(srv.Close)
	return srv, ak, sk
}

type apiTestServer struct {
	t      *testing.T
	server *httptest.Server
	ak     string
	sk     string
}

type apiResponse struct {
	status int
	header http.Header
	body   []byte
}

func newAPITestServer(t *testing.T) *apiTestServer {
	t.Helper()
	srv, ak, sk := setup(t)
	return &apiTestServer{t: t, server: srv, ak: ak, sk: sk}
}

func (s *apiTestServer) do(method, target string, body io.Reader, headers http.Header) apiResponse {
	s.t.Helper()

	requestURL := target
	sign := false
	if !strings.HasPrefix(target, "http://") && !strings.HasPrefix(target, "https://") {
		requestURL = s.server.URL + target
		sign = true
	}

	req, err := http.NewRequest(method, requestURL, body)
	if err != nil {
		s.t.Fatal(err)
	}
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	if sign {
		signRequest(s.t, req, s.ak, s.sk)
	}

	resp, err := s.server.Client().Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		s.t.Fatal(err)
	}
	return apiResponse{status: resp.StatusCode, header: resp.Header.Clone(), body: respBody}
}

func (s *apiTestServer) signed(method, target string, body io.Reader) apiResponse {
	s.t.Helper()
	return s.do(method, target, body, nil)
}

func requireStatus(t *testing.T, got, want int, body []byte) {
	t.Helper()
	if got != want {
		t.Fatalf("want status %d, got %d; body=%s", want, got, body)
	}
}

func TestNoAuthRejected(t *testing.T) {
	srv, _, _ := setup(t)
	resp, err := http.Get(srv.URL + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("want 403, got %d", resp.StatusCode)
	}
}

func TestBucketAPIs(t *testing.T) {
	api := newAPITestServer(t)

	resp := api.signed(http.MethodPut, "/bucket-a", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)
	if location := resp.header.Get("Location"); location != "/bucket-a" {
		t.Fatalf("unexpected bucket location: %q", location)
	}

	resp = api.signed(http.MethodHead, "/bucket-a", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)

	resp = api.signed(http.MethodGet, "/", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)
	if !strings.Contains(string(resp.body), "<Name>bucket-a</Name>") {
		t.Fatalf("list buckets missing bucket-a: %s", resp.body)
	}

	resp = api.signed(http.MethodDelete, "/bucket-a", nil)
	requireStatus(t, resp.status, http.StatusNoContent, resp.body)

	resp = api.signed(http.MethodHead, "/bucket-a", nil)
	requireStatus(t, resp.status, http.StatusNotFound, resp.body)
}

func TestObjectAPIs(t *testing.T) {
	api := newAPITestServer(t)
	requireStatus(t, api.signed(http.MethodPut, "/objects", nil).status, http.StatusOK, nil)

	const body = "hello mini-minio object api"
	resp := api.signed(http.MethodPut, "/objects/path/to/file.txt", strings.NewReader(body))
	requireStatus(t, resp.status, http.StatusOK, resp.body)
	if etag := resp.header.Get("ETag"); etag == "" {
		t.Fatal("PUT object did not return ETag")
	}

	resp = api.signed(http.MethodHead, "/objects/path/to/file.txt", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)
	if got := resp.header.Get("Content-Length"); got != fmt.Sprint(len(body)) {
		t.Fatalf("unexpected Content-Length: %q", got)
	}

	resp = api.signed(http.MethodGet, "/objects/path/to/file.txt", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)
	if string(resp.body) != body {
		t.Fatalf("GET body mismatch: %q", resp.body)
	}

	rangeHeaders := http.Header{"Range": []string{"bytes=6-14"}}
	resp = api.do(http.MethodGet, "/objects/path/to/file.txt", nil, rangeHeaders)
	requireStatus(t, resp.status, http.StatusPartialContent, resp.body)
	if string(resp.body) != "mini-mini" {
		t.Fatalf("range GET body mismatch: %q", resp.body)
	}
	if got := resp.header.Get("Content-Range"); got != "bytes 6-14/27" {
		t.Fatalf("unexpected Content-Range: %q", got)
	}

	resp = api.signed(http.MethodDelete, "/objects/path/to/file.txt", nil)
	requireStatus(t, resp.status, http.StatusNoContent, resp.body)

	resp = api.signed(http.MethodGet, "/objects/path/to/file.txt", nil)
	requireStatus(t, resp.status, http.StatusNotFound, resp.body)
}

func TestMultipartUploadAPI(t *testing.T) {
	api := newAPITestServer(t)
	requireStatus(t, api.signed(http.MethodPut, "/multipart", nil).status, http.StatusOK, nil)

	resp := api.signed(http.MethodPost, "/multipart/large.txt?uploads", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)

	var initResp struct {
		UploadID string `xml:"UploadId"`
	}
	if err := xml.Unmarshal(resp.body, &initResp); err != nil {
		t.Fatal(err)
	}
	if initResp.UploadID == "" {
		t.Fatalf("missing upload id in response: %s", resp.body)
	}

	resp = api.signed(
		http.MethodPut,
		"/multipart/large.txt?partNumber=2&uploadId="+url.QueryEscape(initResp.UploadID),
		strings.NewReader("world"),
	)
	requireStatus(t, resp.status, http.StatusOK, resp.body)

	resp = api.signed(
		http.MethodPut,
		"/multipart/large.txt?partNumber=1&uploadId="+url.QueryEscape(initResp.UploadID),
		strings.NewReader("hello "),
	)
	requireStatus(t, resp.status, http.StatusOK, resp.body)

	completeBody := `<CompleteMultipartUpload><Part><PartNumber>1</PartNumber></Part><Part><PartNumber>2</PartNumber></Part></CompleteMultipartUpload>`
	resp = api.signed(
		http.MethodPost,
		"/multipart/large.txt?uploadId="+url.QueryEscape(initResp.UploadID),
		strings.NewReader(completeBody),
	)
	requireStatus(t, resp.status, http.StatusOK, resp.body)

	resp = api.signed(http.MethodGet, "/multipart/large.txt", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)
	if string(resp.body) != "hello world" {
		t.Fatalf("multipart body mismatch: %q", resp.body)
	}
}

func TestPresignedURLs(t *testing.T) {
	api := newAPITestServer(t)

	// create bucket
	resp := api.signed(http.MethodPut, "/presignbucket", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)

	// generate presigned PUT URL and upload data
	putURL := cmd.PresignPutObject(api.server.URL, "presignbucket", "file.txt", api.ak, api.sk, 10*time.Minute)
	resp = api.signed(http.MethodPut, putURL, strings.NewReader("presigned content"))
	requireStatus(t, resp.status, http.StatusOK, resp.body)

	resp = api.signed(http.MethodPut, putURL+"attacker", strings.NewReader("malicious content"))
	if resp.status == http.StatusOK {
		t.Fatalf("presigned PUT should not allow modifying URL: %d", resp.status)
	}
	// generate presigned GET URL and retrieve data
	getURL := cmd.PresignGetObject(api.server.URL, "presignbucket", "file.txt", api.ak, api.sk, 10*time.Minute)
	resp = api.signed(http.MethodGet, getURL, nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)
	if string(resp.body) != "presigned content" {
		t.Fatalf("body mismatch: %q", resp.body)
	}
}

func TestListObjectsDelimiter(t *testing.T) {
	api := newAPITestServer(t)

	// Create bucket
	resp := api.signed(http.MethodPut, "/testdir", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)

	// Upload objects: some with "/" in name, some without
	objects := []string{"readme.txt", "a/b.txt", "a/c.txt", "a/x/y.txt"}
	for _, obj := range objects {
		resp = api.signed(http.MethodPut, "/testdir/"+obj, strings.NewReader("content of "+obj))
		requireStatus(t, resp.status, http.StatusOK, resp.body)
	}

	// List with no delimiter — should return all objects recursively
	resp = api.signed(http.MethodGet, "/testdir?prefix=&delimiter=", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)
	bodyStr := string(resp.body)
	for _, obj := range objects {
		if !strings.Contains(bodyStr, "<Key>"+obj+"</Key>") {
			t.Errorf("no delimiter: missing %q in response", obj)
		}
	}

	// List with delimiter="/" — should return "readme.txt" as object, "a/" as prefix
	resp = api.signed(http.MethodGet, "/testdir?delimiter=/", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)
	bodyStr = string(resp.body)
	if !strings.Contains(bodyStr, "<Key>readme.txt</Key>") {
		t.Errorf("delimiter=/: missing readme.txt")
	}
	if !strings.Contains(bodyStr, "<Prefix>a/</Prefix>") {
		t.Errorf("delimiter=/: missing prefix a/")
	}

	// List with prefix="a/" and delimiter="/" — should return "a/b.txt", "a/c.txt" as objects, "a/x/" as prefix
	resp = api.signed(http.MethodGet, "/testdir?prefix=a/&delimiter=/", nil)
	requireStatus(t, resp.status, http.StatusOK, resp.body)
	bodyStr = string(resp.body)
	if !strings.Contains(bodyStr, "<Key>a/b.txt</Key>") {
		t.Errorf("prefix=a/ delimiter=/: missing a/b.txt")
	}
	if !strings.Contains(bodyStr, "<Key>a/c.txt</Key>") {
		t.Errorf("prefix=a/ delimiter=/: missing a/c.txt")
	}
	if !strings.Contains(bodyStr, "<Prefix>a/x/</Prefix>") {
		t.Errorf("prefix=a/ delimiter=/: missing prefix a/x/")
	}
}
