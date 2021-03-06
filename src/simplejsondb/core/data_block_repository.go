package core

import (
	"simplejsondb/dbio"
)

type DataBlockRepository interface {
	ControlBlock() ControlBlock
	DataBlocksMap() DataBlocksMap
	RecordBlock(blockID uint16) RecordBlock
	fetchBlock(blockID uint16) *dbio.DataBlock
}

type dataBlockRepository struct {
	buffer dbio.DataBuffer
}

func NewDataBlockRepository(buffer dbio.DataBuffer) DataBlockRepository {
	return &dataBlockRepository{buffer}
}

func (r *dataBlockRepository) ControlBlock() ControlBlock {
	return &controlBlock{r.fetchBlock(0)}
}

func (r *dataBlockRepository) DataBlocksMap() DataBlocksMap {
	return &dataBlocksMap{r.buffer}
}

func (r *dataBlockRepository) RecordBlock(blockID uint16) RecordBlock {
	return &recordBlock{r.fetchBlock(blockID)}
}

func (r *dataBlockRepository) fetchBlock(blockID uint16) *dbio.DataBlock {
	block, err := r.buffer.FetchBlock(blockID)
	if err != nil {
		// If we can't load a block, there's nothing we can do from this point on
		panic(err)
	}
	return block
}
