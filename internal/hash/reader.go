package hash

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"hash"
	"io"
)

type SizeMismatch struct{ Want, Got int64 }

func (e SizeMismatch) Error() string { return "Size mismatch" }

type BadDigest struct{ ExpectedMD5, CalculatedMD5 string }

func (e BadDigest) Error() string { return "Bad digest (MD5 mismatch)" }

// A Reader wraps an io.Reader and computes the MD5 checksum
// of the read content as ETag.
//
// If the reference value for the ETag is not empty then it will
// check whether the computed one matches.
type Reader struct {
	src       io.Reader
	bytesRead int64

	size int64

	checksum  []byte
	md5Hasher hash.Hash
}

// NewReader returns a new Reader that wraps src, limiting reads to size,
// and computes the MD5 checksum of everything it reads as ETag.
//
// When size is >=0 it *must* match the amount of data provided by r.
func NewReader(src io.Reader, size int64, md5Hex string) (*Reader, error) {
	MD5, err := hex.DecodeString(md5Hex)
	if err != nil {
		return nil, BadDigest{ExpectedMD5: md5Hex}
	}
	if size >= 0 {
		src = io.LimitReader(src, size)
	}
	return &Reader{
		src:       src,
		size:      size,
		checksum:  MD5,
		md5Hasher: md5.New(),
	}, nil
}

func (r *Reader) Read(p []byte) (int, error) {
	n, err := r.src.Read(p)
	r.bytesRead += int64(n)

	if n > 0 {
		r.md5Hasher.Write(p[:n])
	}

	if err == io.EOF {
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
	}
	return n, err
}

// Size returns the absolute number of bytes the Reader
// will return during reading. It returns -1 for unlimited
// data.
func (r *Reader) Size() int64 { return r.size }
