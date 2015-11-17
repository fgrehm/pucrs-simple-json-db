package test_utils

import (
	"fmt"
	"simplejsondb/dbio"
)

// An in memory data file does what it says and allows us to avoid hitting
// the FS during tests
type InMemoryDataFile struct {
	Blocks         [][]byte
	CloseFunc      func() error
	ReadBlockFunc  func(uint16, []byte) error
	WriteBlockFunc func(uint16, []byte) error
}

func NewFakeDataFile(blocksCount int) *InMemoryDataFile {
	blocks := [][]byte{}
	for i := 0; i < blocksCount; i++ {
		blocks = append(blocks, make([]byte, dbio.DATABLOCK_SIZE))
	}
	return NewFakeDataFileWithBlocks(blocks)
}

func NewFakeDataFileWithBlocks(blocks [][]byte) *InMemoryDataFile {
	return &InMemoryDataFile{
		Blocks: blocks,
		CloseFunc: func() error {
			return nil // NOOP by default
		},
		WriteBlockFunc: func(id uint16, data []byte) error {
			block := blocks[id]
			for i := range block {
				block[i] = data[i]
			}
			return nil
		},
		ReadBlockFunc: func(id uint16, data []byte) error {
			if id < 0 || id >= uint16(len(blocks)) {
				return fmt.Errorf("Invalid datablock requested: %d", id)
			}
			block := blocks[id]
			for i := 0; i < len(block); i++ {
				data[i] = block[i]
			}
			return nil
		},
	}
}

func (df *InMemoryDataFile) Close() error {
	return df.CloseFunc()
}
func (df *InMemoryDataFile) ReadBlock(id uint16, data []byte) error {
	return df.ReadBlockFunc(id, data)
}
func (df *InMemoryDataFile) WriteBlock(id uint16, data []byte) error {
	return df.WriteBlockFunc(id, data)
}
