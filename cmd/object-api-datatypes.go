package cmd

import (
	"time"
)

// BucketInfo - represents bucket metadata.
type BucketInfo struct {
	// Name of the bucket.
	Name string

	// Date and time when the bucket was created.
	Created time.Time
	Deleted time.Time
}

// ObjectInfo - represents object metadata.
type ObjectInfo struct {
	// Name of the bucket.
	Bucket string

	// Name of the object.
	Name string

	// Date and time when the object was last modified.
	ModTime time.Time

	// Total object size.
	Size int64

	// IsDir indicates if the object is prefix.
	IsDir bool

	// Hex encoded unique entity tag of the object.
	ETag string

	// Version ID of this object.
	VersionID string

	// A standard MIME type describing the format of the object.
	ContentType string

	// Specifies what content encodings have been applied to the object and thus
	// what decoding mechanisms must be applied to obtain the object referenced
	// by the Content-Type header field.
	ContentEncoding string

	// Date and time at which the object is no longer able to be cached
	Expires time.Time

	// List of individual parts, maximum size of upto 10,000
	Parts []ObjectPartInfo `json:"-"`

	// Checksums added on upload.
	// Encoded, maybe encrypted.
	Checksum []byte

	// Inlined
	Inlined bool

	DataBlocks   int
	ParityBlocks int
}

// ObjectPartInfo Info of each part kept in the multipart metadata
// file after CompleteMultipartUpload() is called.
type ObjectPartInfo struct {
	ETag       string            `json:"etag,omitempty" msg:"e"`
	Number     int               `json:"number" msg:"n"`
	Size       int64             `json:"size" msg:"s"`        // Size of the part on the disk.
	ActualSize int64             `json:"actualSize" msg:"as"` // Original size of the part without compression or encryption bytes.
	ModTime    time.Time         `json:"modTime" msg:"mt"`    // Date and time at which the part was uploaded.
	Index      []byte            `json:"index,omitempty" msg:"i,omitempty"`
	Checksums  map[string]string `json:"crc,omitempty" msg:"crc,omitempty"`   // Content Checksums
	Error      string            `json:"error,omitempty" msg:"err,omitempty"` // only set while reading part meta from drive.
}

// ListObjectsV2Info - container for list objects version 2.
type ListObjectsV2Info struct {
	// Indicates whether the returned list objects response is truncated. A
	// value of true indicates that the list was truncated. The list can be truncated
	// if the number of objects exceeds the limit allowed or specified
	// by max keys.
	IsTruncated bool

	// When response is truncated (the IsTruncated element value in the response
	// is true), you can use the key name in this field as marker in the subsequent
	// request to get next set of objects.
	//
	// NOTE: This element is returned only if you have delimiter request parameter
	// specified.
	ContinuationToken     string
	NextContinuationToken string

	// List of objects info for this request.
	Objects []ObjectInfo

	// List of prefixes for this request.
	Prefixes []string
}
