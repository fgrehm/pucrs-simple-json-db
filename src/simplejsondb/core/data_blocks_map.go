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
			isInUse, err := bitMap.Get(i)
			if err != nil {
				panic(err)
			}
			if !isInUse {
				return uint16(i) + blockIndex*dbio.DATABLOCK_SIZE
			}
		}
	}
	return 0
}

func (dbm *dataBlocksMap) MarkAsFree(dataBlockID uint16) {
	dbm.updateBitMap(dataBlockID, func(bitMap dbio.BitMap, flagOffset int) {
		if err := bitMap.Unset(flagOffset); err != nil {
			panic(err)
		}
	})
}

func (dbm *dataBlocksMap) MarkAsUsed(dataBlockID uint16) {
	dbm.updateBitMap(dataBlockID, func(bitMap dbio.BitMap, flagOffset int) {
		if err := bitMap.Set(flagOffset); err != nil {
			panic(err)
		}
	})
}

func (dbm *dataBlocksMap) IsInUse(dataBlockID uint16) bool {
	bitMap, flagOffset := dbm.tupleForBlockID(dataBlockID)

	isInUse, err := bitMap.Get(int(flagOffset))
	if err != nil {
		panic(err)
	}
	return isInUse
}

func (dbm *dataBlocksMap) AllInUse() bool {
	for blockIndex := uint16(0); blockIndex < DATA_BLOCK_MAP_BLOCKS_COUNT; blockIndex++ {
		block, err := dbm.dataBuffer.FetchBlock(DATA_BLOCK_MAP_FIRST_BLOCK + blockIndex)
		if err != nil {
			panic(err)
		}

		bitMap := dbio.NewBitMapFromBytes(block.Data)
		for i := 0; i < dbio.DATABLOCK_SIZE; i++ {
			if isInUse, err := bitMap.Get(i); err != nil {
				panic(err)
			} else if !isInUse {
				return false
			}
		}
	}
	return true
}

func (dbm *dataBlocksMap) updateBitMap(dataBlockID uint16, updateFunc func(dbio.BitMap, int)) {
	updateFunc(dbm.tupleForBlockID(dataBlockID))
	blockOffset := dataBlockID / dbio.DATABLOCK_SIZE
	dbm.dataBuffer.MarkAsDirty(DATA_BLOCK_MAP_FIRST_BLOCK + blockOffset)
}

func (dbm *dataBlocksMap) tupleForBlockID(dataBlockID uint16) (dbio.BitMap, int) {
	blockOffset := dataBlockID / dbio.DATABLOCK_SIZE
	flagOffset := dataBlockID % dbio.DATABLOCK_SIZE

	block, err := dbm.dataBuffer.FetchBlock(DATA_BLOCK_MAP_FIRST_BLOCK + blockOffset)
	if err != nil {
		panic(err)
	}

	return dbio.NewBitMapFromBytes(block.Data), int(flagOffset)
}
