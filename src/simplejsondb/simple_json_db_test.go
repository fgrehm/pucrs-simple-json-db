package simplejsondb_test

import (
	"testing"

	jsondb "simplejsondb"
	"simplejsondb/dbio"

	utils "test_utils"
)

func TestSimpleJSONDB_InitializesDataFile(t *testing.T) {
	firstDataBlock := make([]byte, 10)
	blocksBitMapBlock := make([]byte, dbio.DATABLOCK_SIZE)
	fakeDataFile := utils.NewFakeDataFile([][]byte{firstDataBlock, blocksBitMapBlock})

	jsondb.NewWithDataFile(fakeDataFile)

	if !utils.SlicesEqual(firstDataBlock[0:4], []byte{0x00, 0x00, 0x00, 0x01}) {
		t.Error("Did not set the next id to 1")
	}

	if !utils.SlicesEqual(firstDataBlock[4:6], []byte{0x00, 0x03}) {
		t.Error("Did not set the data block pointer to 3")
	}

	blocksBitMap := dbio.NewBitMapFromBytes(blocksBitMapBlock)
	for i := 0; i < 4; i++ {
		val, err := blocksBitMap.Get(i)
		if err != nil {
			t.Fatal(err)
		}

		if !val {
			t.Errorf("Expected block %d to be flagged as not in use", i)
		}
	}
}
