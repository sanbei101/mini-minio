package cmd_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
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

func TestPresignedURLs(t *testing.T) {
	srv, ak, sk := setup(t)
	client := srv.Client()
	do := func(method, target string, body io.Reader) (int, []byte) {
		var req *http.Request
		var err error
		if strings.HasPrefix(target, "http://") || strings.HasPrefix(target, "https://") {
			req, err = http.NewRequest(method, target, body)
		} else {
			req, err = http.NewRequest(method, srv.URL+target, body)
			if err == nil {
				signRequest(t, req, ak, sk)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
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
	if code, _ := do(http.MethodPut, "/presignbucket", nil); code != http.StatusOK {
		t.Fatalf("create bucket failed: %d", code)
	}

	// generate presigned PUT URL and upload data
	putURL := cmd.PresignPutObject(srv.URL, "presignbucket", "file.txt", ak, sk, 10*time.Minute)
	code, _ := do(http.MethodPut, putURL, strings.NewReader("presigned content"))
	if code != http.StatusOK {
		t.Fatalf("presigned PUT failed: %d", code)
	}
	code, _ = do(http.MethodPut, putURL+"attacker", strings.NewReader("malicious content"))
	if code == http.StatusOK {
		t.Fatalf("presigned PUT should not allow modifying URL: %d", code)
	}
	// generate presigned GET URL and retrieve data
	getURL := cmd.PresignGetObject(srv.URL, "presignbucket", "file.txt", ak, sk, 10*time.Minute)
	code, body := do(http.MethodGet, getURL, nil)
	if code != http.StatusOK {
		t.Fatalf("presigned GET failed: %d", code)
	}
	if string(body) != "presigned content" {
		t.Fatalf("body mismatch: %q", body)
	}
}

func TestListObjectsDelimiter(t *testing.T) {
	srv, ak, sk := setup(t)
	client := srv.Client()

	do := func(method, target string, body io.Reader) (int, []byte) {
		req, err := http.NewRequest(method, srv.URL+target, body)
		if err != nil {
			t.Fatal(err)
		}
		signRequest(t, req, ak, sk)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, respBody
	}

	// Create bucket
	if code, _ := do(http.MethodPut, "/testdir", nil); code != http.StatusOK {
		t.Fatalf("create bucket: %d", code)
	}

	// Upload objects: some with "/" in name, some without
	objects := []string{"readme.txt", "a/b.txt", "a/c.txt", "a/x/y.txt"}
	for _, obj := range objects {
		code, _ := do(http.MethodPut, "/testdir/"+obj, strings.NewReader("content of "+obj))
		if code != http.StatusOK {
			t.Fatalf("PUT %s: %d", obj, code)
		}
	}

	// List with no delimiter — should return all objects recursively
	code, body := do(http.MethodGet, "/testdir?prefix=&delimiter=", nil)
	if code != http.StatusOK {
		t.Fatalf("list no delimiter: %d", code)
	}
	bodyStr := string(body)
	for _, obj := range objects {
		if !strings.Contains(bodyStr, "<Key>"+obj+"</Key>") {
			t.Errorf("no delimiter: missing %q in response", obj)
		}
	}

	// List with delimiter="/" — should return "readme.txt" as object, "a/" as prefix
	code, body = do(http.MethodGet, "/testdir?delimiter=/", nil)
	if code != http.StatusOK {
		t.Fatalf("list delimiter=/ : %d", code)
	}
	bodyStr = string(body)
	if !strings.Contains(bodyStr, "<Key>readme.txt</Key>") {
		t.Errorf("delimiter=/: missing readme.txt")
	}
	if !strings.Contains(bodyStr, "<Prefix>a/</Prefix>") {
		t.Errorf("delimiter=/: missing prefix a/")
	}

	// List with prefix="a/" and delimiter="/" — should return "a/b.txt", "a/c.txt" as objects, "a/x/" as prefix
	code, body = do(http.MethodGet, "/testdir?prefix=a/&delimiter=/", nil)
	if code != http.StatusOK {
		t.Fatalf("list prefix=a/ delimiter=/ : %d", code)
	}
	bodyStr = string(body)
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
