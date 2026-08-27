package cmd

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

const storageRESTPrefix = "/.mini/storage/v1/"

// StorageRESTPrefix is reserved for authenticated cluster drive requests.
func StorageRESTPrefix() string { return storageRESTPrefix }

const (
	storageRESTTimeHeader   = "X-Mini-Storage-Time"
	storageRESTConfigHeader = "X-Mini-Storage-Config"
	storageRESTAuthHeader   = "X-Mini-Storage-Auth"
)

func storageRESTSignature(secret, timestamp, method, escapedPath, rawQuery, configHash string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = fmt.Fprintf(mac, "%s\n%s\n%s\n%s\n%s", timestamp, method, escapedPath, rawQuery, configHash)
	return hex.EncodeToString(mac.Sum(nil))
}

func validStorageRESTRequest(r *http.Request, secret, configHash string) bool {
	if secret == "" || r.Header.Get(storageRESTConfigHeader) != configHash {
		return false
	}
	timestamp := r.Header.Get(storageRESTTimeHeader)
	n, err := strconv.ParseInt(timestamp, 10, 64)
	if err != nil || time.Since(time.Unix(n, 0)) > time.Minute || time.Until(time.Unix(n, 0)) > time.Minute {
		return false
	}
	expected := storageRESTSignature(secret, timestamp, r.Method, r.URL.EscapedPath(), r.URL.RawQuery, configHash)
	return subtle.ConstantTimeCompare([]byte(expected), []byte(r.Header.Get(storageRESTAuthHeader))) == 1
}
