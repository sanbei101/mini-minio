package bpool

import "github.com/klauspost/reedsolomon"

// BytePoolCap is a bounded pool of []byte slices backed by a channel.
// Buffers are 4K-aligned via reedsolomon.AllocAligned for optimal I/O performance.
type BytePoolCap struct {
	c    chan []byte
	w    int // width (usable length)
	wcap int // capacity (actual allocation, 4K-aligned)
}

// NewBytePoolCap creates a pool bounded to maxSize entries,
// with each buffer sized to width (usable) and capwidth (actual capacity).
func NewBytePoolCap(maxSize uint64, width, capwidth int) *BytePoolCap {
	if capwidth <= 0 {
		panic("bpool: capwidth must be > 0")
	}
	if width > capwidth {
		panic("bpool: width cannot exceed capwidth")
	}
	return &BytePoolCap{
		c:    make(chan []byte, maxSize),
		w:    width,
		wcap: capwidth,
	}
}

// Populate pre-fills the pool with aligned buffers. Non-blocking if pool is full.
func (bp *BytePoolCap) Populate() {
	for _, buf := range reedsolomon.AllocAligned(cap(bp.c), bp.wcap) {
		bp.Put(buf[:bp.w])
	}
}

// Get returns a buffer from the pool, or allocates a new aligned buffer.
func (bp *BytePoolCap) Get() []byte {
	if bp == nil {
		return nil
	}
	select {
	case b := <-bp.c:
		return b
	default:
		return reedsolomon.AllocAligned(1, bp.wcap)[0][:bp.w]
	}
}

// Put returns a buffer to the pool. Discards if capacity doesn't match.
func (bp *BytePoolCap) Put(b []byte) {
	if bp == nil {
		return
	}
	if cap(b) != bp.wcap {
		return
	}
	select {
	case bp.c <- b[:bp.w]:
	default:
	}
}

// Width returns the usable buffer length.
func (bp *BytePoolCap) Width() int {
	if bp == nil {
		return 0
	}
	return bp.w
}

// WidthCap returns the actual buffer capacity (4K-aligned).
func (bp *BytePoolCap) WidthCap() int {
	if bp == nil {
		return 0
	}
	return bp.wcap
}
