package simplejsondb_test

import (
	"testing"

	jsondb "simplejsondb"

	utils "test_utils"
)

func TestInitializesDataFile(t *testing.T) {
	firstDataBlock := make([]byte, 10)
	fakeDataFile := utils.NewFakeDataFile([][]byte{firstDataBlock})

	jsondb.NewWithDataFile(fakeDataFile)

	if !utils.SlicesEqual(firstDataBlock[0:4], []byte{0x00, 0x00, 0x00, 0x01}) {
		t.Error("Did not set the next id to 1")
	}

	if !utils.SlicesEqual(firstDataBlock[4:6], []byte{0x00, 0x03}) {
		t.Error("Did not set the data block pointer to 3")
	}
}
