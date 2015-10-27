package core_test

import (
	"simplejsondb/dbio"
	"simplejsondb/core"

	"fmt"

	"testing"
	utils "test_utils"
)

func TestControlBlock_NextID(t *testing.T) {
	block := &dbio.DataBlock{Data: []byte{ 0x00, 0x00, 0x00, 0x10}}
	cb := core.NewControlBlock(block)

	if id := cb.NextID(); id != 16 {
		t.Errorf("Next id was not read, got %d and expected %d", id, 16)
	}

	for i := 0; i < 300; i++ {
		cb.IncNextID()
	}
	if id := cb.NextID(); id != 316 {
		t.Errorf("Next id was not read, got %d and expected %d", id, 316)
	}

	if !utils.SlicesEqual(block.Data, []byte{0x00, 0x00, 0x01, 0x3c}) {
		fmt.Printf("% x\n", block.Data)
		t.Errorf("Invalid data written to block (% x)", block.Data)
	}
}

func TestControlBlock_NextAvailableRecordsDataBlock(t *testing.T) {
	block := &dbio.DataBlock{Data: []byte{0, 0, 0, 0, 0x10, 0x01}}
	cb := core.NewControlBlock(block)

	if id := cb.NextAvailableRecordsDataBlockID(); id != 4097 {
		t.Errorf("Next id was not read, got %d and expected %d", id, 16)
	}

	cb.SetNextAvailableRecordsDataBlockID(900)
	if id := cb.NextAvailableRecordsDataBlockID(); id != 900 {
		t.Errorf("Next id was not read, got %d and expected %d", id, 900)
	}

	if !utils.SlicesEqual(block.Data, []byte{0, 0, 0, 0, 0x03, 0x84}) {
		fmt.Printf("% x\n", block.Data)
		t.Errorf("Invalid data written to block (% x)", block.Data)
	}
}
