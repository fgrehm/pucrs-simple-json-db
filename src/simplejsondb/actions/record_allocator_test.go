package actions_test

import (
	"simplejsondb/actions"
	"simplejsondb/core"
	"simplejsondb/dbio"

	utils "test_utils"
	"testing"
	"fmt"
)

func TestRecordAllocator_BasicAllocation(t *testing.T) {
	blocks := [][]byte{
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		nil,
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
	}
	fakeDataFile := utils.NewFakeDataFile(blocks)
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 4)
	controlDataBlock, _ := dataBuffer.FetchBlock(0)
	core.NewControlBlock(controlDataBlock).Format()
	dataBuffer.MarkAsDirty(controlDataBlock.ID)
	blockMap := core.NewDataBlocksMap(dataBuffer)
	for i := uint16(0); i < 4; i++ {
		blockMap.MarkAsUsed(i)
	}

	allocator := actions.NewRecordAllocator(dataBuffer)

	// Fill up a datablock up to its limit
	maxData := dbio.DATABLOCK_SIZE - core.MIN_UTILIZATION - core.RECORD_HEADER_SIZE
	contents := ""
	for i := uint16(0); i < maxData; i++ {
		contents += fmt.Sprintf("%d", i%10)
	}
	allocator.Run(&core.Record{uint32(1), contents})

	// Add a new record that will go into the next datablock on the list
	allocator.Run(&core.Record{uint32(2), "Some data"})

	// Flush data to data blocks and ensure that things work after a reload
	dataBuffer.Sync()
	dataBuffer = dbio.NewDataBuffer(fakeDataFile, 4)
	blockMap = core.NewDataBlocksMap(dataBuffer)

	// Ensure new blocks has been marked as used
	if !blockMap.IsInUse(3) || !blockMap.IsInUse(4) {
		t.Errorf("Blocks 3 and 4 should have been marked as in use")
	}

	// Ensure the blocks point to each other
	firstRecordDataBlock, _ := dataBuffer.FetchBlock(3)
	firstRecordBlock := core.NewRecordBlock(firstRecordDataBlock)
	if firstRecordBlock.NextBlockID() != 4 {
		t.Errorf("First allocated block does not point to the next one")
	}
	secondRecordDataBlock, _ := dataBuffer.FetchBlock(4)
	secondRecordBlock := core.NewRecordBlock(secondRecordDataBlock)
	if secondRecordBlock.PrevBlockID() != 3 {
		t.Errorf("Second allocated block does not point to the previous one")
	}

	// Ensure the pointer for the next datablock that has free space has been updated
	controlDataBlock, _ = dataBuffer.FetchBlock(0)
	controlBlock := core.NewControlBlock(controlDataBlock)
	if controlBlock.NextAvailableRecordsDataBlockID() != 4 {
		t.Errorf("Did not update the pointer to the next datablock that has allows insertion")
	}
}

func TestRecordAllocator_ChainedRows(t *testing.T) {
	// Fill up a datablock close to its limit

	// Add a new record that will be a chained row

	// Ensure the records can be read after a reload
}
