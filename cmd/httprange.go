package cmd

// HTTPRangeSpec represents a range specification as supported by S3 GET
// object request.
//
// Case 1: Not present -> represented by a nil RangeSpec
// Case 2: bytes=1-10 (absolute start and end offsets) -> RangeSpec{false, 1, 10}
// Case 3: bytes=10- (absolute start offset with end offset unspecified) -> RangeSpec{false, 10, -1}
// Case 4: bytes=-30 (suffix length specification) -> RangeSpec{true, -30, -1}
type HTTPRangeSpec struct {
	// Does the range spec refer to a suffix of the object?
	IsSuffixLength bool

	// Start and end offset specified in range spec
	Start, End int64
}
