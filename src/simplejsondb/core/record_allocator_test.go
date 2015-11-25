package core_test

import (
	"simplejsondb/core"
	"simplejsondb/dbio"

	"fmt"
	utils "test_utils"
	"testing"
)

func TestRecordAllocator_Add(t *testing.T) {
	fakeDataFile := utils.NewFakeDataFile(7)
	if err := core.FormatDataFileIfNeeded(fakeDataFile); err != nil {
		t.Fatal(err)
	}
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 4)
	allocator := core.NewRecordAllocator(dataBuffer)

	// Fill up a datablock up to its limit
	maxData := dbio.DATABLOCK_SIZE - core.MIN_UTILIZATION - core.RECORD_HEADER_SIZE
	contents := ""
	for i := uint16(0); i < maxData; i++ {
		contents += fmt.Sprintf("%d", i%10)
	}
	allocator.Add(&core.Record{ID: uint32(1), Data: []byte(contents)})

	// Add a new record that will go into the next datablock on the list
	allocator.Add(&core.Record{ID: uint32(2), Data: []byte("Some data")})

	// Flush data to data blocks and ensure that things work after a reload
	dataBuffer.Sync()
	dataBuffer = dbio.NewDataBuffer(fakeDataFile, 4)
	repo := core.NewDataBlockRepository(dataBuffer)
	blockMap := repo.DataBlocksMap()

	// Ensure new blocks has been marked as used
	if !blockMap.IsInUse(3) || !blockMap.IsInUse(4) {
		t.Errorf("Blocks 3 and 4 should have been marked as in use")
	}

	// Ensure the blocks point to each other
	firstRecordBlock := repo.RecordBlock(3)
	if firstRecordBlock.NextBlockID() != 4 {
		t.Errorf("First allocated block does not point to the next one")
	}
	secondRecordBlock := repo.RecordBlock(4)
	if secondRecordBlock.PrevBlockID() != 3 {
		t.Errorf("Second allocated block does not point to the previous one")
	}

	// Ensure the pointer for the next datablock that has free space has been updated
	controlBlock := repo.ControlBlock()
	if controlBlock.NextAvailableRecordsDataBlockID() != 4 {
		t.Errorf("Did not update the pointer to the next datablock that allows insertion, got %d", controlBlock.NextAvailableRecordsDataBlockID())
	}
}

func TestRecordAllocator_Remove(t *testing.T) {
	fakeDataFile := utils.NewFakeDataFile(8)
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 5)
	if err := core.FormatDataFileIfNeeded(fakeDataFile); err != nil {
		t.Fatal(err)
	}

	allocator := core.NewRecordAllocator(dataBuffer)

	// Prepare data to fill up a datablock up to its limit
	maxData := dbio.DATABLOCK_SIZE - core.MIN_UTILIZATION - core.RECORD_HEADER_SIZE
	contents := ""
	for i := uint16(0); i < maxData; i++ {
		contents += fmt.Sprintf("%d", i%10)
	}

	// Insert data into 3 different blocks
	allocator.Add(&core.Record{ID: uint32(3), Data: []byte(contents)})
	allocator.Add(&core.Record{ID: uint32(4), Data: []byte(contents)})
	allocator.Add(&core.Record{ID: uint32(5), Data: []byte(contents)})
	allocator.Add(&core.Record{ID: uint32(6), Data: []byte("Some data")})
	allocator.Add(&core.Record{ID: uint32(7), Data: []byte("More data")})

	// Free up some datablocks
	allocator.Remove(core.RowID{DataBlockID: 3, LocalID: 0})
	allocator.Remove(core.RowID{DataBlockID: 5, LocalID: 0})

	// Free part of another datablock
	allocator.Remove(core.RowID{DataBlockID: 6, LocalID: 0})

	// Flush data to data blocks and ensure that things work after a reload
	dataBuffer.Sync()

	dataBuffer = dbio.NewDataBuffer(fakeDataFile, 4)
	repo := core.NewDataBlockRepository(dataBuffer)
	blockMap := repo.DataBlocksMap()

	// Ensure blocks have been marked as free again
	if blockMap.IsInUse(3) {
		t.Errorf("Block 3 should have been marked as free")
	}
	if blockMap.IsInUse(5) {
		t.Errorf("Block 5 should have been marked as free")
	}

	// Ensure the linked list is set up properly
	// First records datablock is now at block 4
	controlBlock := repo.ControlBlock()
	if controlBlock.FirstRecordDataBlock() != 4 {
		t.Fatalf("First record datablock is set to the wrong block, found %d", controlBlock.FirstRecordDataBlock())
	}

	// Then the next block on the chain is at block 6
	recordBlock := repo.RecordBlock(4)
	if recordBlock.NextBlockID() != 6 {
		t.Fatalf("First record datablock next block pointer is set to the wrong block (%d)", recordBlock.NextBlockID())
	}

	// And the block 6 points back to the block 4
	recordBlock = repo.RecordBlock(6)
	if recordBlock.PrevBlockID() != 4 {
		t.Fatalf("Second record datablock previous block pointer is incorrect (%d)", recordBlock.PrevBlockID())
	}
}

func TestRecordAllocator_Update(t *testing.T) {
	fakeDataFile := utils.NewFakeDataFile(6)
	if err := core.FormatDataFileIfNeeded(fakeDataFile); err != nil {
		t.Fatal(err)
	}

	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 4)
	allocator := core.NewRecordAllocator(dataBuffer)

	// Fill up a datablock up to its limit
	maxData := dbio.DATABLOCK_SIZE - core.MIN_UTILIZATION - core.RECORD_HEADER_SIZE
	contents := ""
	for i := uint16(0); i < maxData; i++ {
		contents += fmt.Sprintf("%d", i%10)
	}
	allocator.Add(&core.Record{ID: 1, Data: []byte(contents)})

	// Add a new record that will go into the next datablock on the list
	allocator.Add(&core.Record{ID: 2, Data: []byte("Some data")})

	// Update records
	rowID := core.RowID{DataBlockID: 3, LocalID: 0}
	if err := allocator.Update(rowID, &core.Record{ID: 1, Data: []byte("NEW CONTENTS")}); err != nil {
		t.Fatal(err)
	}
	rowID = core.RowID{DataBlockID: 4, LocalID: 0}
	if err := allocator.Update(rowID, &core.Record{ID: 2, Data: []byte("EVEN MORE!")}); err != nil {
		t.Fatal(err)
	}

	// Flush data to data blocks and ensure that things work after a reload
	dataBuffer.Sync()
	dataBuffer = dbio.NewDataBuffer(fakeDataFile, 4)
	repo := core.NewDataBlockRepository(dataBuffer)

	// Ensure blocks have been updated
	recordBlock := repo.RecordBlock(3)
	data, err := recordBlock.ReadRecordData(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "NEW CONTENTS" {
		t.Errorf("First record did not get updated, read `%s`", data)
	}

	recordBlock = repo.RecordBlock(4)
	data, err = recordBlock.ReadRecordData(0)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "EVEN MORE!" {
		t.Errorf("Second record did not get updated, read `%s`", data)
	}
}

func TestRecordAllocator_ChainedRows(t *testing.T) {
	fakeDataFile := utils.NewFakeDataFile(11)
	dataBuffer := dbio.NewDataBuffer(fakeDataFile, 5)
	if err := core.FormatDataFileIfNeeded(fakeDataFile); err != nil {
		t.Fatal(err)
	}

	allocator := core.NewRecordAllocator(dataBuffer)

	// Prepare data to fill up a datablock close to its limit
	maxData := dbio.DATABLOCK_SIZE - core.MIN_UTILIZATION - core.RECORD_HEADER_SIZE
	contents := ""
	for i := uint16(0); i < maxData; i++ {
		contents += fmt.Sprintf("%d", i%10)
	}

	// Insert data into 3 different blocks
	dummy, _ := allocator.Add(&core.Record{ID: uint32(3), Data: []byte(contents[0 : maxData-100])})
	chainedRowRowID, _ := allocator.Add(&core.Record{ID: uint32(4), Data: []byte(contents)})
	removedChainedRowID, _ := allocator.Add(&core.Record{ID: uint32(5), Data: []byte(contents)})
	allocator.Add(&core.Record{ID: uint32(6), Data: []byte("Some data")})
	allocator.Add(&core.Record{ID: uint32(7), Data: []byte("More data")})

	// Ensure that the blocks are chained
	if dummy.DataBlockID != chainedRowRowID.DataBlockID {
		t.Fatalf("Did not create a chained row, expected record to be written on block %d but was written on block %d", dummy.DataBlockID, chainedRowRowID.DataBlockID)
	}

	// Ensure we exercise the code path that deletes chained rows
	allocator.Remove(dummy)
	allocator.Remove(removedChainedRowID)

	// Flush data to data blocks and ensure that things work after a reload
	dataBuffer.Sync()
	dataBuffer = dbio.NewDataBuffer(fakeDataFile, 10)
	repo := core.NewDataBlockRepository(dataBuffer)
	allocator = core.NewRecordAllocator(dataBuffer)

	// Ensure the records can be read after a reload
	recordBlock := repo.RecordBlock(chainedRowRowID.DataBlockID)
	first, err := recordBlock.ReadRecordData(chainedRowRowID.LocalID)
	if err != nil {
		t.Fatal(err)
	}
	chainedRowID, err := recordBlock.ChainedRowID(chainedRowRowID.LocalID)
	if err != nil {
		t.Fatal(err)
	}
	recordBlock = repo.RecordBlock(chainedRowID.DataBlockID)
	second, err := recordBlock.ReadRecordData(chainedRowID.LocalID)
	if string(first)+string(second) != contents {
		t.Errorf("Invalid contents found for record, found `%s` and `%s`, expected `%s`", first, second, contents)
	}

	// Ensure deletes clear out headers properly
	recordBlock = repo.RecordBlock(removedChainedRowID.DataBlockID)
	if _, err = recordBlock.ReadRecordData(removedChainedRowID.LocalID); err == nil {
		t.Fatal("Did not clear out the record header of one of the a chained rows deleted")
	}
	recordBlock = repo.RecordBlock(removedChainedRowID.DataBlockID + 1)
	if _, err = recordBlock.ReadRecordData(0); err == nil {
		t.Fatal("Did not clear out the record header of the next block of the chained row")
	}

	dataBuffer.Sync()
	dataBuffer = dbio.NewDataBuffer(fakeDataFile, 10)
	repo = core.NewDataBlockRepository(dataBuffer)
	allocator = core.NewRecordAllocator(dataBuffer)

	// Add and update a chained row that spans 3 blocks
	bigContents := contents + contents + contents
	chainedUpdateRowID, _ := allocator.Add(&core.Record{ID: uint32(9), Data: []byte(bigContents)})

	// Keep track of the list of the following row ids of the chained row
	rowIDs := []core.RowID{}
	recordBlock = repo.RecordBlock(chainedUpdateRowID.DataBlockID)
	nextRowID, err := recordBlock.ChainedRowID(chainedUpdateRowID.LocalID)
	if err != nil {
		t.Fatal(err)
	}
	rowIDs = append(rowIDs, nextRowID)

	recordBlock = repo.RecordBlock(nextRowID.DataBlockID)
	nextRowID, err = recordBlock.ChainedRowID(nextRowID.LocalID)
	if err != nil {
		t.Fatal(err)
	}
	rowIDs = append(rowIDs, nextRowID)
	if len(rowIDs) != 2 {
		t.Errorf("Spread record on more blocks than expected %+v", rowIDs)
	}

	// Change record to be really small
	allocator.Update(chainedUpdateRowID, &core.Record{ID: uint32(9), Data: []byte("a string")})

	// Ensure the next element on the chained row list got cleared
	for _, rowID := range rowIDs {
		_, err := repo.RecordBlock(rowID.DataBlockID).ReadRecordData(rowID.LocalID)
		if err == nil {
			t.Errorf("Did not clear chained row %+v", rowID)
		}
	}

	// Ensure we can read it
	recordBlock = repo.RecordBlock(chainedUpdateRowID.DataBlockID)
	data, _ := recordBlock.ReadRecordData(chainedRowID.LocalID)
	if string(data) != "a string" {
		t.Error("Invalid contents found for record")
	}
}
