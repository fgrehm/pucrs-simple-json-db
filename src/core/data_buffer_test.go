package core_test

import (
	"errors"
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

func TestWithBlockExecutesCallbackWithDataBlockAndReturnsInternalError(t *testing.T) {
	fakeError := errors.New("An error")
	fakeBlock := []byte{0x12}
	fakeDataFile := newFakeDataFile([][]byte{fakeBlock})

	err := core.NewDataBuffer(fakeDataFile, 1).WithBlock(0, func(block *core.Datablock) error {
		if !slicesEqual(block.Data[0:1], fakeBlock) {
			t.Error("Unknown block returned")
		}
		return fakeError
	})
	if err != fakeError {
		t.Error("Unknown error returned")
	}
}

func TestWithBlockReturnsReadErrorWhenItHappens(t *testing.T) {
	fakeError := errors.New("An error")
	fakeDataFile := newFakeDataFile([][]byte{[]byte{}})
	fakeDataFile.readBlockFunc = func(id uint16, data []byte) error {
		return fakeError
	}

	err := core.NewDataBuffer(fakeDataFile, 1).WithBlock(0, func(block *core.Datablock) error { return nil })
	if err != fakeError {
		t.Error("Unknown error returned")
	}
}

func TestSavesDirtyFramesWhenEvicting(t *testing.T) {
	t.Fatal("TODO: Implement this behavior")
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
