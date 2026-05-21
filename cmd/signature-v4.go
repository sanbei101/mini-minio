package cmd

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

const (
	signV4Algorithm = "AWS4-HMAC-SHA256"
	iso8601Format   = "20060102T150405Z"
	yyyymmdd        = "20060102"
	unsignedPayload = "UNSIGNED-PAYLOAD"
	region          = "us-east-1"
	service         = "s3"
)

// Credentials holds the server's access/secret key pair.
type Credentials struct {
	AccessKey string
	SecretKey string
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

func hashSHA256(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

// signingKey derives the SigV4 signing key.
func signingKey(secretKey string, t time.Time) []byte {
	date := hmacSHA256([]byte("AWS4"+secretKey), []byte(t.Format(yyyymmdd)))
	reg := hmacSHA256(date, []byte(region))
	svc := hmacSHA256(reg, []byte(service))
	return hmacSHA256(svc, []byte("aws4_request"))
}

func scope(t time.Time) string {
	return t.Format(yyyymmdd) + "/" + region + "/" + service + "/aws4_request"
}

// canonicalQueryString returns the sorted, encoded query string for signing.
// excludeSig removes X-Amz-Signature from the output (used when building the string-to-sign for presigned URLs).
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

// verifyHeaderAuth validates an Authorization: AWS4-HMAC-SHA256 header.
func verifyHeaderAuth(r *http.Request, creds Credentials) error {
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, signV4Algorithm+" ") {
		return fmt.Errorf("missing or unsupported Authorization header")
	}
	// Parse: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...
	// Clients may use ", " or "," as separator.
	rest := strings.TrimPrefix(auth, signV4Algorithm+" ")
	credStr := extractAuthField(rest, "Credential")
	signedHeadersStr := extractAuthField(rest, "SignedHeaders")
	signature := extractAuthField(rest, "Signature")
	if credStr == "" || signedHeadersStr == "" || signature == "" {
		return fmt.Errorf("malformed Authorization header")
	}

	credParts := strings.Split(credStr, "/")
	if len(credParts) < 5 {
		return fmt.Errorf("malformed Credential")
	}
	if credParts[0] != creds.AccessKey {
		return fmt.Errorf("unknown access key")
	}
	t, err := time.Parse(iso8601Format, r.Header.Get("X-Amz-Date"))
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Date")
	}

	signedHeaders := strings.Split(signedHeadersStr, ";")
	hdr := make(http.Header)
	for _, k := range signedHeaders {
		if k == "host" {
			hdr.Set("host", r.Host)
		} else {
			hdr[http.CanonicalHeaderKey(k)] = r.Header[http.CanonicalHeaderKey(k)]
		}
	}
	canonHdr, signedStr := canonicalHeaders(hdr, signedHeaders)

	payloadHash := r.Header.Get("X-Amz-Content-Sha256")
	if payloadHash == "" {
		payloadHash = unsignedPayload
	}

	canonReq := strings.Join([]string{
		r.Method,
		r.URL.EscapedPath(),
		canonicalQueryString(r.URL.Query(), false),
		canonHdr,
		signedStr,
		payloadHash,
	}, "\n")

	stringToSign := signV4Algorithm + "\n" +
		t.Format(iso8601Format) + "\n" +
		scope(t) + "\n" +
		hashSHA256([]byte(canonReq))

	key := signingKey(creds.SecretKey, t)
	expected := hex.EncodeToString(hmacSHA256(key, []byte(stringToSign)))

	if subtle.ConstantTimeCompare([]byte(expected), []byte(signature)) != 1 {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

// verifyPresignedAuth validates a presigned SigV4 query-string request.
func verifyPresignedAuth(r *http.Request, creds Credentials) error {
	q := r.URL.Query()

	if q.Get("X-Amz-Algorithm") != signV4Algorithm {
		return fmt.Errorf("unsupported algorithm")
	}
	credStr := q.Get("X-Amz-Credential")
	credParts := strings.Split(credStr, "/")
	if len(credParts) < 5 || credParts[0] != creds.AccessKey {
		return fmt.Errorf("unknown access key")
	}
	t, err := time.Parse(iso8601Format, q.Get("X-Amz-Date"))
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Date")
	}
	expires, err := time.ParseDuration(q.Get("X-Amz-Expires") + "s")
	if err != nil {
		return fmt.Errorf("invalid X-Amz-Expires")
	}
	if time.Since(t) > expires {
		return fmt.Errorf("presigned URL expired")
	}

	signedHeaders := strings.Split(q.Get("X-Amz-SignedHeaders"), ";")
	hdr := make(http.Header)
	for _, k := range signedHeaders {
		if k == "host" {
			hdr.Set("host", r.Host)
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
		return fmt.Errorf("signature mismatch")
	}
	return nil
}

// PresignURL generates a SigV4 presigned URL for the given method/bucket/object.
func PresignURL(baseURL, method, bucket, object, accessKey, secretKey string, expiry time.Duration) string {
	t := time.Now().UTC()
	host := strings.TrimPrefix(strings.TrimPrefix(baseURL, "https://"), "http://")

	q := url.Values{}
	q.Set("X-Amz-Algorithm", signV4Algorithm)
	q.Set("X-Amz-Credential", accessKey+"/"+scope(t))
	q.Set("X-Amz-Date", t.Format(iso8601Format))
	q.Set("X-Amz-Expires", fmt.Sprintf("%d", int(expiry.Seconds())))
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

// extractAuthField extracts a named field from the Authorization header body.
// Handles both ", " and "," as field separators.
func extractAuthField(s, field string) string {
	prefix := field + "="
	for _, part := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' }) {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, prefix) {
			return strings.TrimPrefix(part, prefix)
		}
	}
	return ""
}
