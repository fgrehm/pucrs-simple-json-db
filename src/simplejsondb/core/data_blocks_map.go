package core

import (
	"simplejsondb/dbio"
)

type DataBlocksMap interface {
	AllInUse() bool
	FirstFree() uint16
	IsInUse(dataBlockID uint16) bool
	MarkAsFree(dataBlockID uint16)
	MarkAsUsed(dataBlockID uint16)
}

type dataBlocksMap struct {
	dataBuffer dbio.DataBuffer
}

const (
	DATA_BLOCK_MAP_FIRST_BLOCK  = uint16(1)
	DATA_BLOCK_MAP_BLOCKS_COUNT = uint16(2)
)

func NewDataBlocksMap(dataBuffer dbio.DataBuffer) DataBlocksMap {
	return &dataBlocksMap{dataBuffer}
}

func (dbm *dataBlocksMap) FirstFree() uint16 {
	for blockIndex := uint16(0); blockIndex < DATA_BLOCK_MAP_BLOCKS_COUNT; blockIndex++ {
		block, err := dbm.dataBuffer.FetchBlock(DATA_BLOCK_MAP_FIRST_BLOCK + blockIndex)
		if err != nil {
			panic(err)
		}

		bitMap := dbio.NewBitMapFromBytes(block.Data)
		for i := 0; i < dbio.DATABLOCK_SIZE; i++ {
			set, err := bitMap.Get(i)
			if err != nil {
				panic(err)
			}
			if !set {
				return uint16(i) + blockIndex*dbio.DATABLOCK_SIZE
			}
		}
	}
	return 0
}

func (dbm *dataBlocksMap) MarkAsFree(dataBlockID uint16) {
	blockOffset := dataBlockID / dbio.DATABLOCK_SIZE
	flagOffset := dataBlockID % dbio.DATABLOCK_SIZE

	block, err := dbm.dataBuffer.FetchBlock(DATA_BLOCK_MAP_FIRST_BLOCK + blockOffset)
	if err != nil {
		panic(err)
	}
	bm := dbio.NewBitMapFromBytes(block.Data)

	if err = bm.Unset(int(flagOffset)); err != nil {
		panic(err)
	}

	dbm.dataBuffer.MarkAsDirty(block.ID)
}

func (dbm *dataBlocksMap) MarkAsUsed(dataBlockID uint16) {
	blockOffset := dataBlockID / dbio.DATABLOCK_SIZE
	flagOffset := dataBlockID % dbio.DATABLOCK_SIZE

	block, err := dbm.dataBuffer.FetchBlock(DATA_BLOCK_MAP_FIRST_BLOCK + blockOffset)
	if err != nil {
		panic(err)
	}
	bm := dbio.NewBitMapFromBytes(block.Data)

	if err = bm.Set(int(flagOffset)); err != nil {
		panic(err)
	}

	dbm.dataBuffer.MarkAsDirty(block.ID)
}

func (dbm *dataBlocksMap) IsInUse(dataBlockID uint16) bool {
	blockOffset := dataBlockID / dbio.DATABLOCK_SIZE
	flagOffset := dataBlockID % dbio.DATABLOCK_SIZE

	block, err := dbm.dataBuffer.FetchBlock(DATA_BLOCK_MAP_FIRST_BLOCK + blockOffset)
	if err != nil {
		panic(err)
	}
	bm := dbio.NewBitMapFromBytes(block.Data)

	set, err := bm.Get(int(flagOffset))
	if err != nil {
		panic(err)
	}
	return set
}

func (dbm *dataBlocksMap) AllInUse() bool {
	for blockIndex := uint16(0); blockIndex < DATA_BLOCK_MAP_BLOCKS_COUNT; blockIndex++ {
		block, err := dbm.dataBuffer.FetchBlock(DATA_BLOCK_MAP_FIRST_BLOCK + blockIndex)
		if err != nil {
			panic(err)
		}

		bitMap := dbio.NewBitMapFromBytes(block.Data)
		for i := 0; i < dbio.DATABLOCK_SIZE; i++ {
			set, err := bitMap.Get(i)
			if err != nil {
				panic(err)
			}
			if !set {
				return false
			}
		}
	}
	return true
}
