// Based on some code wrote by Shawn Milochik [1] tweaked for easier
// serialization into binary files
//
// [1] - https://github.com/ShawnMilo/bitmap/raw/master/bitmap.go

package dbio

import "errors"

var ErrOutOfRange = errors.New("Value out of range.")

// BitMap is a struct containing a slice of bytes,
// being used as a bitmap.
type BitMap struct {
	size int
	vals []byte
}

// NewBitMap returns a BitMap. It requires a size. A bitmap with a size of
// eight or less will be one byte in size, and so on.
func NewBitMap(s int) BitMap {
	l := s / 8
	return BitMap{size: s, vals: make([]byte, l, l)}
}

func NewBitMapFromBytes(vals []byte) BitMap {
	return BitMap{size: len(vals) * 8, vals: vals}
}

// Size returns the size of a bitmap. This is the number
// of bits.
func (b BitMap) Size() int {
	return b.size
}

// checkRange returns an error if the position
// passed is not allowed.
func (b BitMap) checkRange(i int) error {
	if i > b.Size() {
		return ErrOutOfRange
	}
	if i < 0 {
		return ErrOutOfRange
	}
	return nil
}

// For internal use; drives Set and Unset.
func (b BitMap) toggle(i int) {
	// Position of the byte in b.vals.
	p := i >> 3
	// Position of the bit in the byte.
	remainder := i - (p * 8)
	// Toggle the bit.
	if remainder == 0 {
		b.vals[p] = b.vals[p] ^ 1
	} else {
		b.vals[p] = b.vals[p] ^ (1 << uint(remainder-1))
	}
}

// Set sets a position in
// the bitmap to 1.
func (b BitMap) Set(i int) error {
	if x := b.checkRange(i); x != nil {
		return x
	}
	// Don't unset.
	val, err := b.Get(i)
	if err != nil {
		return err
	}
	if val {
		return nil
	}
	b.toggle(i)
	return nil
}

// Unset sets a position in
// the bitmap to 0.
func (b BitMap) Unset(i int) error {
	// Don't set.
	val, err := b.Get(i)
	if err != nil {
		return err
	}
	if val {
		b.toggle(i)
	}
	return nil
}

// VBytes returns the underlying slice of bytes
// representing the bitmap. Used for persisting
// on binary files.
func (b BitMap) Bytes() []byte {
	return b.vals
}

// Get returns a boolean indicating whether
// the bit is set for the position in question.
func (b BitMap) Get(i int) (bool, error) {
	if x := b.checkRange(i); x != nil {
		return false, x
	}
	p := i >> 3
	remainder := i - (p * 8)
	if remainder == 0 {
		return b.vals[p] > b.vals[p]^1, nil
	}
	return b.vals[p] > b.vals[p]^(1<<uint(remainder-1)), nil
}
