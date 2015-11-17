package dbio_test

import (
	"errors"
	"testing"

	"simplejsondb/dbio"

	utils "test_utils"
)

func TestFetchesBlockFromDataFile(t *testing.T) {
	fakeDataBlock := []byte{0x10, 0xF0}
	fakeDataFile := utils.NewFakeDataFileWithBlocks([][]byte{nil, fakeDataBlock})

	dataBlock, err := dbio.NewDataBuffer(fakeDataFile, 1).FetchBlock(1)
	if err != nil {
		t.Fatal("Unexpected error", err)
	}
	if dataBlock.ID != 1 {
		t.Errorf("ID doesn't match (expected %d got %d)", 1, dataBlock.ID)
	}
	if !utils.SlicesEqual(dataBlock.Data[0:2], fakeDataBlock) {
		t.Errorf("Data blocks do not match (expected %x got %x)", fakeDataBlock, dataBlock.Data[0:2])
	}
}

func TestFetchBlockCachesData(t *testing.T) {
	fakeDataFile := utils.NewFakeDataFileWithBlocks([][]byte{nil, []byte{}})

	readCount := 0
	original := fakeDataFile.ReadBlockFunc
	fakeDataFile.ReadBlockFunc = func(id uint16, data []byte) error {
		readCount += 1
		return original(id, data)
	}

	buffer := dbio.NewDataBuffer(fakeDataFile, 1)
	for i := 0; i < 10; i++ {
		buffer.FetchBlock(1)
	}

	if readCount > 1 {
		t.Errorf("Read from datafile more than once (total: %d times)", readCount)
	}
}

func TestEvictsBlocksAfterFillingInAllFrames(t *testing.T) {
	fakeDataFile := utils.NewFakeDataFileWithBlocks([][]byte{
		[]byte{}, []byte{}, []byte{},
	})

	readCount := 0
	original := fakeDataFile.ReadBlockFunc
	fakeDataFile.ReadBlockFunc = func(id uint16, data []byte) error {
		readCount += 1
		return original(id, data)
	}

	buffer := dbio.NewDataBuffer(fakeDataFile, 2)

	// From this point on, we fetch blocks in a way to ensure that we have multiple
	// hits on different blocks and also force a couple cache misses

	for blockId := 0; blockId < 3; blockId++ {
		for i := 0; i < 10; i++ {
			buffer.FetchBlock(uint16(blockId))
		}
	}
	// Fetch block 1 again to ensure it is still in memory
	buffer.FetchBlock(uint16(1))
	if readCount != 3 {
		t.Errorf("Read from datafile more than three times (total: %d times)", readCount)
	}

	// Force 2 cache misses
	buffer.FetchBlock(0)
	buffer.FetchBlock(1)
	if readCount != 5 {
		t.Error("Read from cache but should not do that")
	}
}

func TestSavesDirtyFramesWhenEvicting(t *testing.T) {
	fakeDataBlock := []byte{0x00, 0x01, 0x02}
	fakeDataFile := utils.NewFakeDataFileWithBlocks([][]byte{
		fakeDataBlock, []byte{}, []byte{},
	})

	blockThatWasWritten := uint16(999)
	bytesWritten := []byte{}
	fakeDataFile.WriteBlockFunc = func(id uint16, data []byte) error {
		blockThatWasWritten = id
		bytesWritten = data
		return nil
	}

	buffer := dbio.NewDataBuffer(fakeDataFile, 2)

	// Read the first 2 blocks and flag the first one as dirty
	buffer.FetchBlock(0)
	buffer.FetchBlock(1)
	buffer.MarkAsDirty(0)

	// Evict the first frame (by loading a third frame)
	buffer.FetchBlock(2)

	if blockThatWasWritten == 999 {
		t.Fatal("Block was not saved to disk")
	}
	if blockThatWasWritten != 0 {
		t.Errorf("Unknown block saved to disk (%d)", blockThatWasWritten)
	}
	if !utils.SlicesEqual(bytesWritten[0:3], fakeDataBlock) {
		t.Errorf("Invalid data saved to disk %x", bytesWritten[0:3])
	}
}

func TestDiscardsUnmodifiedFrames(t *testing.T) {
	fakeDataFile := utils.NewFakeDataFileWithBlocks([][]byte{
		[]byte{}, []byte{}, []byte{},
	})

	wroteToDisk := false
	fakeDataFile.WriteBlockFunc = func(id uint16, data []byte) error {
		wroteToDisk = true
		return nil
	}

	buffer := dbio.NewDataBuffer(fakeDataFile, 2)

	// Read the first 2 blocks
	buffer.FetchBlock(0)
	buffer.FetchBlock(1)

	// Evict the first frame (by loading a third frame)
	buffer.FetchBlock(2)

	if wroteToDisk {
		t.Fatal("No blocks should have been saved to disk")
	}
}

func TestSavesDirtyFramesOnSync(t *testing.T) {
	fakeDataFile := utils.NewFakeDataFileWithBlocks([][]byte{
		[]byte{}, []byte{}, []byte{},
	})

	blocksThatWereWritten := []uint16{}
	fakeDataFile.WriteBlockFunc = func(id uint16, data []byte) error {
		blocksThatWereWritten = append(blocksThatWereWritten, id)
		return nil
	}

	buffer := dbio.NewDataBuffer(fakeDataFile, 3)

	// Read the 3 blocks and flag two as dirty
	buffer.FetchBlock(0)
	buffer.MarkAsDirty(0)

	buffer.FetchBlock(1)
	// Not dirty

	buffer.FetchBlock(2)
	buffer.MarkAsDirty(2)

	if err := buffer.Sync(); err != nil {
		t.Fatal(err)
	}

	if len(blocksThatWereWritten) != 2 {
		t.Fatalf("Should have written 2 blocks, wrote %v", blocksThatWereWritten)
	}

	if blocksThatWereWritten[0] != 0 && blocksThatWereWritten[1] != 0 {
		t.Errorf("Should have written the block 0, wrote %v", blocksThatWereWritten)
	}

	if blocksThatWereWritten[0] != 2 && blocksThatWereWritten[1] != 2 {
		t.Errorf("Should have written the block 2, wrote %v", blocksThatWereWritten)
	}

	blocksThatWereWritten = []uint16{}
	if err := buffer.Sync(); err != nil {
		t.Fatal(err)
	}

	if len(blocksThatWereWritten) != 0 {
		t.Fatalf("Blocks have already been writen, wrote %v again", blocksThatWereWritten)
	}
}

func TestReturnsErrorsWhenSyncing(t *testing.T) {
	fakeDataFile := utils.NewFakeDataFileWithBlocks([][]byte{
		[]byte{}, []byte{},
	})
	expectedError := errors.New("BOOM")
	fakeDataFile.WriteBlockFunc = func(id uint16, data []byte) error {
		return expectedError
	}

	buffer := dbio.NewDataBuffer(fakeDataFile, 2)

	buffer.FetchBlock(0)
	buffer.FetchBlock(1)
	if err := buffer.Sync(); err != nil {
		t.Fatal("Unexpected error", err)
	}

	buffer.MarkAsDirty(1)
	err := buffer.Sync()
	if err == nil {
		t.Fatal("Error not raised")
	} else if err != expectedError {
		t.Fatal("Unknown error raised")
	}
}
