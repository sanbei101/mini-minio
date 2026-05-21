package hash

import (
	"bytes"
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
)

type SizeMismatch struct{ Want, Got int64 }

func (e SizeMismatch) Error() string { return "Size mismatch" }

type BadDigest struct{ ExpectedMD5, CalculatedMD5 string }

func (e BadDigest) Error() string { return "Bad digest (MD5 mismatch)" }

type SHA256Mismatch struct{ ExpectedSHA256, CalculatedSHA256 string }

func (e SHA256Mismatch) Error() string { return "SHA256 mismatch" }

// A Reader wraps an io.Reader and computes the MD5 checksum
// of the read content as ETag. Optionally, it also computes
// the SHA256 checksum of the content.
//
// If the reference values for the ETag and content SHA26
// are not empty then it will check whether the computed
// match the reference values.
type Reader struct {
	src         io.Reader
	bytesRead   int64
	expectedMin int64
	expectedMax int64

	size       int64
	actualSize int64

	checksum      []byte
	contentSHA256 []byte

	disableMD5 bool

	md5Hasher    hash.Hash
	sha256Hasher hash.Hash
}

// NewReader returns a new Reader that wraps src and computes
// MD5 checksum of everything it reads as ETag.
//
// It also computes the SHA256 checksum of everything it reads
// if sha256Hex is not the empty string.
//
// If size resp. actualSize is unknown at the time of calling
// NewReader then it should be set to -1.
// When size is >=0 it *must* match the amount of data provided by r.
//
// NewReader may try merge the given size, MD5 and SHA256 values
// into src - if src is a Reader - to avoid computing the same
// checksums multiple times.
// NewReader enforces S3 compatibility strictly by ensuring caller
// does not send more content than specified size.
func NewReader(src io.Reader, size int64, md5Hex, sha256Hex string, actualSize int64) (*Reader, error) {
	MD5, err := hex.DecodeString(md5Hex)
	if err != nil {
		return nil, BadDigest{
			ExpectedMD5:   md5Hex,
			CalculatedMD5: "",
		}
	}
	SHA256, err := hex.DecodeString(sha256Hex)
	if err != nil {
		return nil, SHA256Mismatch{
			ExpectedSHA256:   sha256Hex,
			CalculatedSHA256: "",
		}
	}

	// NewReader enforces S3 compatibility strictly by ensuring caller
	// does not send more content than specified size.
	if size >= 0 {
		src = io.LimitReader(src, size)
	}

	return &Reader{
		src:           src,
		size:          size,
		actualSize:    actualSize,
		checksum:      MD5,
		contentSHA256: SHA256,
		md5Hasher:     md5.New(),
		sha256Hasher:  sha256.New(),
		disableMD5:    false,
	}, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	r.bytesRead += int64(n)

	if n > 0 {
		r.md5Hasher.Write(p[:n])
		r.sha256Hasher.Write(p[:n])
	}

	if err == io.EOF { // Verify content SHA256, if set.
		if r.size >= 0 && r.bytesRead != r.size {
			return n, SizeMismatch{Want: r.size, Got: r.bytesRead}
		}

		if len(r.checksum) > 0 {
			sum := r.md5Hasher.Sum(nil)
			if !bytes.Equal(r.checksum, sum) {
				return n, BadDigest{
					ExpectedMD5:   hex.EncodeToString(r.checksum),
					CalculatedMD5: hex.EncodeToString(sum),
				}
			}
		}

		if len(r.contentSHA256) > 0 {
			sum := r.sha256Hasher.Sum(nil)
			if !bytes.Equal(r.contentSHA256, sum) {
				return n, SHA256Mismatch{
					ExpectedSHA256:   hex.EncodeToString(r.contentSHA256),
					CalculatedSHA256: hex.EncodeToString(sum),
				}
			}
		}
	}
	return n, err
}

// Size returns the absolute number of bytes the Reader
// will return during reading. It returns -1 for unlimited
// data.
func (r *Reader) Size() int64 { return r.size }

// ActualSize returns the pre-modified size of the object.
// DecompressedSize - For compressed objects.
func (r *Reader) ActualSize() int64 { return r.actualSize }

// MD5CurrentHexString 获取到目前为止读出的内容的 MD5 字符串（兼容外部 ETag 的直接调用）
func (r *Reader) MD5CurrentHexString() string {
	return hex.EncodeToString(r.md5Hasher.Sum(nil))
}

// Close and release resources.
func (r *Reader) Close() error { return nil }
