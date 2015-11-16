package simplejsondb_test

import (
	"testing"

	jsondb "simplejsondb"
	// "simplejsondb/core"
	"simplejsondb/dbio"

	utils "test_utils"
)

func TestSimpleJSONDB_InitializesDataFile(t *testing.T) {
	firstDataBlock := make([]byte, 10)
	blocksBitMapBlock := make([]byte, dbio.DATABLOCK_SIZE)
	bTreeRootBlock := make([]byte, 2)
	fakeDataFile := utils.NewFakeDataFile([][]byte{
		firstDataBlock,
		blocksBitMapBlock,
		nil,
		nil,
		bTreeRootBlock,
	})

	jsondb.NewWithDataFile(fakeDataFile)

	if !utils.SlicesEqual(firstDataBlock[0:2], []byte{0x00, 0x03}) {
		t.Error("Did not set the next available data block pointer to 3")
	}

	if !utils.SlicesEqual(firstDataBlock[2:4], []byte{0x00, 0x03}) {
		t.Error("Did not set the first record block pointer to 3")
	}

	if !utils.SlicesEqual(firstDataBlock[4:6], []byte{0x00, 0x04}) {
		t.Error("Did not set the btree pointer to 4")
	}

	blocksBitMap := dbio.NewBitMapFromBytes(blocksBitMapBlock)
	for i := 0; i < 5; i++ {
		val, err := blocksBitMap.Get(i)
		if err != nil {
			t.Fatal(err)
		}

		if !val {
			t.Errorf("Expected block %d to be flagged as used", i)
		}
	}

	// if bTreeRootBlock[0] != core.BTREE_TYPE_LEAF {
	// 	println(bTreeRootBlock[0])
	// 	t.Error("Did not initialize the root node of the btree index as a leaf node")
	// }
}
