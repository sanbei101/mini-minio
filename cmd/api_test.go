package cmd_test

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
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

	canonReq := strings.Join([]string{
		req.Method,
		req.URL.EscapedPath(),
		req.URL.RawQuery,
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

func TestBucketAndObjectCRUD(t *testing.T) {
	srv, ak, sk := setup(t)
	client := srv.Client()

	do := func(method, path string, body io.Reader) (int, []byte) {
		req, err := http.NewRequest(method, srv.URL+path, body)
		if err != nil {
			t.Fatal(err)
		}
		signRequest(t, req, ak, sk)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, respBody
	}

	// create bucket
	if code, _ := do(http.MethodPut, "/mybucket", nil); code != http.StatusOK {
		t.Fatalf("create bucket failed: %d", code)
	}

	// put object
	if code, _ := do(http.MethodPut, "/mybucket/hello.txt", strings.NewReader("hello world")); code != http.StatusOK {
		t.Fatalf("put object failed: %d", code)
	}

	// get object
	code, body := do(http.MethodGet, "/mybucket/hello.txt", nil)
	if code != http.StatusOK {
		t.Fatalf("get object failed: %d", code)
	}
	if string(body) != "hello world" {
		t.Fatalf("body mismatch: %q", body)
	}

	// list objects
	code, listBody := do(http.MethodGet, "/mybucket", nil)
	if code != http.StatusOK {
		t.Fatalf("list objects failed: %d", code)
	}
	if !bytes.Contains(listBody, []byte("hello.txt")) {
		t.Fatalf("hello.txt not in list: %s", listBody)
	}

	// delete object
	if code, _ := do(http.MethodDelete, "/mybucket/hello.txt", nil); code != http.StatusNoContent {
		t.Fatalf("delete object failed: %d", code)
	}

	// delete bucket
	if code, _ := do(http.MethodDelete, "/mybucket", nil); code != http.StatusNoContent {
		t.Fatalf("delete bucket failed: %d", code)
	}
}

func TestPresignedURLs(t *testing.T) {
	srv, ak, sk := setup(t)
	client := srv.Client()

	// First create bucket with signed request
	req, _ := http.NewRequest("PUT", srv.URL+"/presignbucket", nil)
	// req.Host = strings.TrimPrefix(srv.URL, "http://")
	signRequest(t, req, ak, sk)
	r, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		t.Fatal("create bucket failed")
	}
	t.Logf("Create bucket response: %v", r)
	// Generate presigned PUT URL
	putURL := cmd.PresignPutObject(srv.URL, "presignbucket", "file.txt", ak, sk, 10*time.Minute)
	putReq, _ := http.NewRequest("PUT", putURL, strings.NewReader("presigned content"))
	r, err = client.Do(putReq)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Presigned PUT response: %v", r)
	if r.StatusCode != 200 {
		body, _ := io.ReadAll(r.Body)
		t.Fatalf("presigned PUT: %d — %s", r.StatusCode, body)
	}

	badURL := putURL + "attacker"
	badReq, _ := http.NewRequest("PUT", badURL, strings.NewReader("malicious content"))
	r, err = client.Do(badReq)
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode == 200 {
		t.Fatalf("expected failure for tampered URL, got 200")
	}
	// Generate presigned GET URL
	getURL := cmd.PresignGetObject(srv.URL, "presignbucket", "file.txt", ak, sk, 10*time.Minute)
	getURL = strings.Replace(getURL, "localhost:9000", strings.TrimPrefix(srv.URL, "http://"), 1)

	getReq, _ := http.NewRequest("GET", getURL, nil)
	getReq.Host = strings.TrimPrefix(srv.URL, "http://")
	r, err = client.Do(getReq)
	if err != nil {
		t.Fatal(err)
	}
	if r.StatusCode != 200 {
		body, _ := io.ReadAll(r.Body)
		t.Fatalf("presigned GET: %d — %s", r.StatusCode, body)
	}
	body, _ := io.ReadAll(r.Body)
	if string(body) != "presigned content" {
		t.Fatalf("body mismatch: %q", body)
	}
}
