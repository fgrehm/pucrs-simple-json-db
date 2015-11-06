package core

import (
	"simplejsondb/dbio"
)

type DataBlockRepository interface {
	ControlBlock() ControlBlock
	DataBlocksMap() DataBlocksMap
	RecordBlock(blockID uint16) RecordBlock
	BTreeNode(blockID uint16) BTreeNode
	BTreeLeaf(blockID uint16) BTreeLeaf
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

func (r *dataBlockRepository) BTreeNode(blockID uint16) BTreeNode {
	node := &bTreeNode{r.fetchBlock(blockID)}
	if node.IsLeaf() {
		return &bTreeLeaf{node}
	} else {
		return &bTreeBranch{node}
	}
}

func (r *dataBlockRepository) BTreeLeaf(blockID uint16) BTreeLeaf {
	return &bTreeLeaf{&bTreeNode{r.fetchBlock(blockID)}}
}

func (r *dataBlockRepository) fetchBlock(blockID uint16) *dbio.DataBlock {
	block, err := r.buffer.FetchBlock(blockID)
	if err != nil {
		// If we can't load a block, there's nothing we can do from this point on
		panic(err)
	}
	return block
}
