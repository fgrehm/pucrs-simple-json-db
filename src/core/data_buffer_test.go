package core_test

import (
	"testing"

	"core"
)

func TestFetchesBlockFromDataFile(t *testing.T) {
	fakeDataBlock := &core.Datablock{ID: 1, Data: []byte{0x10, 0xF0}}
	fakeDataFile := newFakeDataFile([]*core.Datablock{nil, fakeDataBlock})

	dataBlock, err := core.NewDataBuffer(fakeDataFile, 1).FetchBlock(1)
	if err != nil {
		t.Fatal("Unexpected error", err)
	}
	if dataBlock.ID != 1 {
		t.Errorf("ID doesn't match (expected %d got %d)", 1, dataBlock.ID)
	}
	if !slicesEqual(dataBlock.Data, fakeDataBlock.Data) {
		t.Errorf("Data blocks do not match (expected %x got %x)", 1, fakeDataBlock.Data, dataBlock.Data)
	}
}

func TestFetchBlockCachesTheResult(t *testing.T) {
	fakeDataBlock := &core.Datablock{ID: 1, Data: []byte{}}
	fakeDataFile := newFakeDataFile([]*core.Datablock{nil, fakeDataBlock})

	readCount := 0
	original := fakeDataFile.readBlockFunc
	fakeDataFile.readBlockFunc = func(id uint16) (*core.Datablock, error) {
		readCount += 1
		return original(id)
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
	fakeDataFile := newFakeDataFile([]*core.Datablock{
		&core.Datablock{ID: 0, Data: []byte{}},
		&core.Datablock{ID: 1, Data: []byte{}},
		&core.Datablock{ID: 2, Data: []byte{}},
	})

	readCount := 0
	original := fakeDataFile.readBlockFunc
	fakeDataFile.readBlockFunc = func(id uint16) (*core.Datablock, error) {
		readCount += 1
		return original(id)
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
	t.Fatal("TODO: Implement this behavior")
}

type inMemoryDataFile struct {
	blocks         []*core.Datablock
	closeFunc      func()
	readBlockFunc  func(uint16) (*core.Datablock, error)
	writeBlockFunc func(*core.Datablock) error
}

func newFakeDataFile(blocks []*core.Datablock) *inMemoryDataFile {
	return &inMemoryDataFile{
		blocks:    blocks,
		closeFunc: func() {}, // NOOP by default
		readBlockFunc: func(id uint16) (*core.Datablock, error) {
			return blocks[id], nil
		},
	}
}

func (df *inMemoryDataFile) Close() {
	df.closeFunc()
}
func (df *inMemoryDataFile) ReadBlock(id uint16) (*core.Datablock, error) {
	return df.readBlockFunc(id)
}
func (df *inMemoryDataFile) WriteBlock(block *core.Datablock) error {
	return df.writeBlockFunc(block)
}
