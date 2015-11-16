package core

import (
	"simplejsondb/dbio"

	"fmt"

	utils "test_utils"
	"testing"
)

func TestControlBlock_NextAvailableRecordsDataBlock(t *testing.T) {
	block := &dbio.DataBlock{Data: []byte{0x10, 0x01}}
	cb := &controlBlock{block}

	if id := cb.NextAvailableRecordsDataBlockID(); id != 4097 {
		t.Errorf("Next id was not read, got %d and expected %d", id, 16)
	}

	cb.SetNextAvailableRecordsDataBlockID(900)
	if id := cb.NextAvailableRecordsDataBlockID(); id != 900 {
		t.Errorf("Next id was not read, got %d and expected %d", id, 900)
	}

	if !utils.SlicesEqual(block.Data, []byte{0x03, 0x84}) {
		fmt.Printf("% x\n", block.Data)
		t.Errorf("Invalid data written to block (% x)", block.Data)
	}
}

func TestControlBlock_BTreeRootBlock(t *testing.T) {
	block := &dbio.DataBlock{Data: []byte{0, 0, 0, 0, 0, 0x09}}
	cb := &controlBlock{block}

	if blockID := cb.BTreeRootBlock(); blockID != 9 {
		t.Errorf("Root BTree datablock pointer was not read, got %d and expected %d", blockID, 9)
	}

	cb.SetBTreeRootBlock(901)
	if id := cb.BTreeRootBlock(); id != 901 {
		t.Errorf("Next id was not read, got %d and expected %d", id, 901)
	}

	if !utils.SlicesEqual(block.Data, []byte{0, 0, 0, 0, 0x03, 0x85}) {
		fmt.Printf("% x\n", block.Data)
		t.Errorf("Invalid data written to block (% x)", block.Data)
	}
}
