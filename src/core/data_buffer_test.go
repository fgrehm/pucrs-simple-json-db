package core_test

import (
	"testing"

	"core"
)

func TestFetchesBlockFromDataFile(t *testing.T) {
	fakeDataBlock := []byte{0x10, 0xF0}
	fakeDataFile := newFakeDataFile([][]byte{nil, fakeDataBlock})

	dataBlock, err := core.NewDataBuffer(fakeDataFile, 1).FetchBlock(1)
	if err != nil {
		t.Fatal("Unexpected error", err)
	}
	if dataBlock.ID != 1 {
		t.Errorf("ID doesn't match (expected %d got %d)", 1, dataBlock.ID)
	}
	if !slicesEqual(dataBlock.Data[0:2], fakeDataBlock) {
		t.Errorf("Data blocks do not match (expected %x got %x)", fakeDataBlock, dataBlock.Data[0:2])
	}
}

func TestFetchBlockCachesTheResult(t *testing.T) {
	fakeDataFile := newFakeDataFile([][]byte{nil, []byte{}})

	readCount := 0
	original := fakeDataFile.readBlockFunc
	fakeDataFile.readBlockFunc = func(id uint16, data []byte) error {
		readCount += 1
		return original(id, data)
	}

	buffer := core.NewDataBuffer(fakeDataFile, 1)
	for i := 0; i < 10; i++ {
		buffer.FetchBlock(1)
	}

	if readCount > 1 {
		t.Errorf("Read from datafile more than once (total: %d times)", readCount)
	}
}

func TestEvictsCachedBlocksAfterFillingInAllFrames(t *testing.T) {
	fakeDataFile := newFakeDataFile([][]byte{
		[]byte{}, []byte{}, []byte{},
	})

	readCount := 0
	original := fakeDataFile.readBlockFunc
	fakeDataFile.readBlockFunc = func(id uint16, data []byte) error {
		readCount += 1
		return original(id, data)
	}

	buffer := core.NewDataBuffer(fakeDataFile, 2)

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
	fakeDataFile := newFakeDataFile([][]byte{
		fakeDataBlock, []byte{}, []byte{},
	})

	blockThatWasWritten := uint16(999)
	bytesWritten := []byte{}
	fakeDataFile.writeBlockFunc = func(id uint16, data []byte) error {
		blockThatWasWritten = id
		bytesWritten = data
		return nil
	}

	buffer := core.NewDataBuffer(fakeDataFile, 2)

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
	if !slicesEqual(bytesWritten[0:3], fakeDataBlock) {
		t.Errorf("Invalid data saved to disk %x", bytesWritten[0:3])
	}
}

func TestDoesNotWriteFramesWhenEvictingIfItHasntBeenModified(t *testing.T) {
	fakeDataFile := newFakeDataFile([][]byte{
		[]byte{}, []byte{}, []byte{},
	})

	wroteToDisk := false
	fakeDataFile.writeBlockFunc = func(id uint16, data []byte) error {
		wroteToDisk = true
		return nil
	}

	buffer := core.NewDataBuffer(fakeDataFile, 2)

	// Read the first 2 blocks and flag the first one as dirty
	buffer.FetchBlock(0)
	buffer.FetchBlock(1)

	// Evict the first frame (by loading a third frame)
	buffer.FetchBlock(2)

	if wroteToDisk {
		t.Fatal("No blocks should have been saved to disk")
	}
}

type inMemoryDataFile struct {
	blocks         [][]byte
	closeFunc      func()
	readBlockFunc  func(uint16, []byte) error
	writeBlockFunc func(uint16, []byte) error
}

func newFakeDataFile(blocks [][]byte) *inMemoryDataFile {
	return &inMemoryDataFile{
		blocks:    blocks,
		closeFunc: func() {}, // NOOP by default
		writeBlockFunc: func(id uint16, data []byte) error {
			// NOOP by default
			return nil
		},
		readBlockFunc: func(id uint16, data []byte) error {
			block := blocks[id]
			for i := 0; i < len(block); i++ {
				data[i] = block[i]
			}
			return nil
		},
	}
}

func (df *inMemoryDataFile) Close() {
	df.closeFunc()
}
func (df *inMemoryDataFile) ReadBlock(id uint16, data []byte) error {
	return df.readBlockFunc(id, data)
}
func (df *inMemoryDataFile) WriteBlock(id uint16, data []byte) error {
	return df.writeBlockFunc(id, data)
}
