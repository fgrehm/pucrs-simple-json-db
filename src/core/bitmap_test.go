// Based on some code wrote by Shawn Milochik [1] tweaked for easier
// serialization into binary files
//
// [1] - https://github.com/ShawnMilo/bitmap/raw/master/bitmap_test.go

package core_test

import (
	"fmt"
	"log"
	"testing"

	"core"
)

// get gets a value from a bitmap, handling
// all the error checking so it's not repeated
// a million times.
func get(b core.BitMap, i int) bool {
	val, err := b.Get(i)
	if err != nil {
		log.Fatal("error getting from bitmap", err)
	}
	return val
}

// TestCreate proves we can create a
// bitmap and its size is as set.
func TestCreate(t *testing.T) {
	for size := range []int{1, 13, 27, 66} {
		b := core.NewBitMap(size)
		if b.Size() != size {
			t.Error("size doesn't match")
		}
	}
}

func TestSet(t *testing.T) {
	b := core.NewBitMap(16)
	b.Set(2)
	b.Set(15)

	if !slicesEqual(b.Bytes(), []byte{0x02, 0x40}) {
		t.Errorf("bytes do not match '%X'", b.Bytes())
	}
}

func TestGet(t *testing.T) {
	b := core.NewBitMap(50)
	b.Set(2)
	if !get(b, 2) {
		t.Error("expected true, got false")
	}
	if get(b, 3) {
		t.Error("expected false, got true")
	}
	b.Set(3)
	if !get(b, 3) {
		t.Error("expected true, got false")
	}
	// Larger number (to prove indexing past the first
	// byte works).
	if get(b, 42) {
		t.Error("expected false, got true")
	}
	b.Set(42)
	if !get(b, 42) {
		t.Error("expected true, got false")
	}
}

// Setting a value twice should not unset it.
func TestSetTwice(t *testing.T) {
	b := core.NewBitMap(10)
	b.Set(2)
	if !get(b, 2) {
		t.Error("expected it to be set")
	}
	b.Set(2)
	if !get(b, 2) {
		t.Error("expected it to still be set")
	}
}

// Unset a value.
func TestSetUnset(t *testing.T) {
	b := core.NewBitMap(10)
	err := b.Set(2)
	if err != nil {
		t.Error(err)
	}
	if !get(b, 2) {
		t.Error("expected it to be set")
	}
	err = b.Unset(2)
	if err != nil {
		t.Error(err)
	}
	if get(b, 2) {
		t.Error("expected it to be unset")
	}
	b.Unset(2)
	if get(b, 2) {
		t.Error("expected it to still be unset")
	}
}

func ExampleBitmap() {
	b := core.NewBitMap(10)
	b.Set(2)
	fmt.Printf("2 in bitmap: %v. 7 in bitmap: %v.\n", get(b, 2), get(b, 7))
	// Output: 2 in bitmap: true. 7 in bitmap: false.
}

// TestSetOverflow tests dealing with out-of-range issues
// in the Set method.
func TestSetOverflow(t *testing.T) {
	b := core.NewBitMap(42)
	err := b.Set(52)
	if err != core.ErrOutOfRange {
		t.Error("out of range, there should be an error")
	}
	err = b.Set(-1)
	if err != core.ErrOutOfRange {
		t.Error("out of range, there should be an error")
	}
}

// TestGetOverflow tests dealing with out-of-range issues
// in the Get method.
func TestGetOverflow(t *testing.T) {
	b := core.NewBitMap(42)
	_, err := b.Get(52)
	if err != core.ErrOutOfRange {
		t.Error("out of range, there should be an error")
	}
	err = b.Set(-1)
	if err != core.ErrOutOfRange {
		t.Error("out of range, there should be an error")
	}
}
