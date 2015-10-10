package core_test

import (
	"testing"

	"core"
)

func TestInitializesDataFile(t *testing.T) {
	firstDataBlock := make([]byte, 10)
	fakeDataFile := newFakeDataFile([][]byte{firstDataBlock})

	core.NewMetaDBWithDataFile(fakeDataFile)

	if !slicesEqual(firstDataBlock[0:8], []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}) {
		t.Error("Did not set the next id to 1")
	}

	if !slicesEqual(firstDataBlock[8:10], []byte{0x00, 0x01}) {
		t.Error("Did not set the data block pointer to 1")
	}
}
