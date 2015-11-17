package core

import (
	"bplustree"
	"simplejsondb/dbio"
)

type DataBlockRepository interface {
	ControlBlock() ControlBlock
	DataBlocksMap() DataBlocksMap
	RecordBlock(blockID uint16) RecordBlock
	IndexNode(nodeID uint16) bplustree.Node
	IndexBranch(nodeID uint16) bplustree.BranchNode
	IndexLeaf(nodeID uint16) bplustree.LeafNode
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

func (r *dataBlockRepository) IndexNode(nodeID uint16) bplustree.Node {
	node := &uint32IndexNode{r.fetchBlock(nodeID)}
	if node.isLeaf() {
		return &uint32IndexLeafNode{node}
	} else {
		return &uint32IndexBranchNode{node}
	}
}

func (r *dataBlockRepository) IndexBranch(nodeID uint16) bplustree.BranchNode {
	return &uint32IndexBranchNode{&uint32IndexNode{r.fetchBlock(nodeID)}}
}

func (r *dataBlockRepository) IndexLeaf(nodeID uint16) bplustree.LeafNode {
	return &uint32IndexLeafNode{&uint32IndexNode{r.fetchBlock(nodeID)}}
}

func (r *dataBlockRepository) fetchBlock(blockID uint16) *dbio.DataBlock {
	block, err := r.buffer.FetchBlock(blockID)
	if err != nil {
		// If we can't load a block, there's nothing we can do from this point on
		panic(err)
	}
	return block
}
