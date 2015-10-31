package actions_test

import (
	"simplejsondb/actions"
	"simplejsondb/core"
	"simplejsondb/dbio"

	"fmt"
	utils "test_utils"
	"testing"
)

func TestRecordAllocator_Add(t *testing.T) {
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
	allocator.Add(&core.Record{uint32(1), contents})

	// Add a new record that will go into the next datablock on the list
	allocator.Add(&core.Record{uint32(2), "Some data"})

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

func TestRecordAllocator_Remove(t *testing.T) {
	blocks := [][]byte{
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		nil,
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
	}
	fakeDataFile := utils.NewFakeDataFile(blocks)
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 10)
	controlDataBlock, _ := dataBuffer.FetchBlock(0)
	core.NewControlBlock(controlDataBlock).Format()
	dataBuffer.MarkAsDirty(controlDataBlock.ID)
	blockMap := core.NewDataBlocksMap(dataBuffer)
	for i := uint16(0); i < 4; i++ {
		blockMap.MarkAsUsed(i)
	}

	allocator := actions.NewRecordAllocator(dataBuffer)

	// Prepare data to fill up a datablock up to its limit
	maxData := dbio.DATABLOCK_SIZE - core.MIN_UTILIZATION - core.RECORD_HEADER_SIZE
	contents := ""
	for i := uint16(0); i < maxData; i++ {
		contents += fmt.Sprintf("%d", i%10)
	}

	// Insert data into 3 different blocks
	allocator.Add(&core.Record{uint32(3), contents})
	allocator.Add(&core.Record{uint32(4), contents})
	allocator.Add(&core.Record{uint32(5), contents})
	allocator.Add(&core.Record{uint32(6), "Some data"})
	allocator.Add(&core.Record{uint32(7), "More data"})

	// Free up some datablocks
	allocator.Remove(core.RowID{DataBlockID: 3, LocalID: 0})
	allocator.Remove(core.RowID{DataBlockID: 5, LocalID: 0})

	// Free part of another datablock
	allocator.Remove(core.RowID{DataBlockID: 6, LocalID: 0})

	// Flush data to data blocks and ensure that things work after a reload
	dataBuffer.Sync()

	dataBuffer = dbio.NewDataBuffer(fakeDataFile, 4)
	blockMap = core.NewDataBlocksMap(dataBuffer)

	// Ensure blocks have been marked as free again
	if blockMap.IsInUse(3) {
		t.Errorf("Block 3 should have been marked as free")
	}
	if blockMap.IsInUse(5) {
		t.Errorf("Block 5 should have been marked as free")
	}

	// Ensure the linked list is set up properly
	// First records datablock is now at block 4
	controlDataBlock, _ = dataBuffer.FetchBlock(0)
	controlBlock := core.NewControlBlock(controlDataBlock)
	if controlBlock.FirstRecordDataBlock() != 4 {
		t.Fatal("First record datablock is set to the wrong block")
	}

	// Then the next block on the chain is at block 6
	dataBlock, _ := dataBuffer.FetchBlock(4)
	recordBlock := core.NewRecordBlock(dataBlock)
	if recordBlock.NextBlockID() != 6 {
		t.Fatal("First record datablock next block pointer is set to the wrong block")
	}

	// And the block 6 points back to the block 4
	dataBlock, _ = dataBuffer.FetchBlock(6)
	recordBlock = core.NewRecordBlock(dataBlock)
	if recordBlock.PrevBlockID() != 4 {
		t.Fatal("Second record datablock previous block pointer is incorrect")
	}
}

func TestRecordAllocator_ChainedRows(t *testing.T) {

	blocks := [][]byte{
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		nil,
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
		make([]byte, dbio.DATABLOCK_SIZE),
	}
	fakeDataFile := utils.NewFakeDataFile(blocks)
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 10)
	controlDataBlock, _ := dataBuffer.FetchBlock(0)
	core.NewControlBlock(controlDataBlock).Format()
	dataBuffer.MarkAsDirty(controlDataBlock.ID)
	blockMap := core.NewDataBlocksMap(dataBuffer)
	for i := uint16(0); i < 4; i++ {
		blockMap.MarkAsUsed(i)
	}

	allocator := actions.NewRecordAllocator(dataBuffer)

	// Prepare data to fill up a datablock close to its limit
	maxData := dbio.DATABLOCK_SIZE - core.MIN_UTILIZATION - core.RECORD_HEADER_SIZE
	maxData -= 100
	contents := ""
	for i := uint16(0); i < maxData; i++ {
		contents += fmt.Sprintf("%d", i%10)
	}

	// Insert data into 3 different blocks
	dummy, _ := allocator.Add(&core.Record{uint32(3), contents})
	removedChainedRowID, _ := allocator.Add(&core.Record{uint32(4), contents})
	chainedRowID, _ := allocator.Add(&core.Record{uint32(5), contents})
	allocator.Add(&core.Record{uint32(6), "Some data"})
	allocator.Add(&core.Record{uint32(7), "More data"})

	// Ensure that the blocks are chained
	if dummy.DataBlockID != removedChainedRowID.DataBlockID {
		t.Fatal("Did not create a chained row")
	}

	// Ensure we exercise the code path that deletes chained rows
	allocator.Remove(dummy)
	allocator.Remove(removedChainedRowID)

	// Flush data to data blocks and ensure that things work after a reload
	dataBuffer.Sync()
	dataBuffer = dbio.NewDataBuffer(fakeDataFile, 4)
	blockMap = core.NewDataBlocksMap(dataBuffer)

	// Ensure the records can be read after a reload
	dataBlock, _ := dataBuffer.FetchBlock(chainedRowID.DataBlockID)
	recordBlock := core.NewRecordBlock(dataBlock)
	data, err := recordBlock.ReadRecordData(chainedRowID.LocalID)
	if err != nil {
		t.Fatal(err)
	}
	if data != contents {
		t.Error("Invalid contents found for record")
	}

	// Ensure deletes clear out headers properly
	dataBlock, _ = dataBuffer.FetchBlock(removedChainedRowID.DataBlockID)
	recordBlock = core.NewRecordBlock(dataBlock)
	if _, err = recordBlock.ReadRecordData(removedChainedRowID.LocalID); err == nil {
		t.Fatal("Did not clear out the record header of one of the a chained rows deleted")
	}
}
