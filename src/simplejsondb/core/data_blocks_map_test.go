package core

import (
	"simplejsondb/dbio"

	"testing"

	utils "test_utils"
)

func TestDataBlocksMap(t *testing.T) {
	blocks := [][]byte{
		nil,
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
	}
	fakeDataFile := utils.NewFakeDataFile(blocks)
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 2)
	dbm := &dataBlocksMap{dataBuffer}

	// First position is free by default
	if free := dbm.FirstFree(); free != 0 {
		t.Errorf("Unexpected result for fetching the first free block")
	}

	// Mark some blocks as being in use
	dbm.MarkAsUsed(0)
	dbm.MarkAsUsed(1)
	dbm.MarkAsUsed(2)
	dbm.MarkAsUsed(5)
	dbm.MarkAsUsed(dbio.DATABLOCK_SIZE + 100)

	// Ensure it spots the gap
	if free := dbm.FirstFree(); free != 3 {
		t.Errorf("Unexpected result for fetching the first free block after some interaction with the map")
	}

	// Ensure it reclaims the free block
	dbm.MarkAsFree(2)
	if free := dbm.FirstFree(); free != 2 {
		t.Errorf("Did not reclaim the new free block")
	}

	// Ensure it works across data blocks
	if !dbm.IsInUse(1) {
		t.Errorf("Expected datablock 1 to be in use")
	}
	if !dbm.IsInUse(dbio.DATABLOCK_SIZE + 100) {
		t.Errorf("Expected datablock %d to be in use", dbio.DATABLOCK_SIZE+100)
	}

	// Fill in the whole map
	max := dbio.DATABLOCK_SIZE * 2
	for i := 0; i < max; i++ {
		dbm.MarkAsUsed(uint16(i))
	}
	// Ensure it detects that there are no more available blocks
	if !dbm.AllInUse() {
		t.Error("Expected all positions to be in use")
	}
	dbm.MarkAsFree(2)
	if dbm.AllInUse() {
		t.Error("Expected all positions to not be in use")
	}

	// Ensure that the blocks / frames were flagged as dirty
	blocksThatWereWritten := []uint16{}
	fakeDataFile.WriteBlockFunc = func(id uint16, data []byte) error {
		blocksThatWereWritten = append(blocksThatWereWritten, id)
		return nil
	}
	dataBuffer.Sync()
	if len(blocksThatWereWritten) != 2 {
		t.Fatalf("Should have written 2 blocks, wrote %v", blocksThatWereWritten)
	}
}
