package core

// We might want to extract the specific node types out to separate files  if
// the code grows too big

import (
	"simplejsondb/dbio"
)

const (
	BTREE_TYPE_BRANCH = uint8(1)
	BTREE_TYPE_LEAF   = uint8(2)
	BTREE_POS_TYPE    = 0
)

type BTreeNode interface {
	DataBlockID() uint16
	IsLeaf() bool
}

type bTreeNode struct {
	block *dbio.DataBlock
}

func (n *bTreeNode) DataBlockID() uint16 {
	return n.block.ID
}

func (n *bTreeNode) IsLeaf() bool {
	return n.block.ReadUint8(BTREE_POS_TYPE) == BTREE_TYPE_LEAF
}


type BTreeBranch interface {
	BTreeNode
}

type bTreeBranch struct {
	*bTreeNode
}

func CreateBTreeBranch(block *dbio.DataBlock) BTreeBranch {
	block.Write(BTREE_POS_TYPE, BTREE_TYPE_BRANCH)
	node := &bTreeNode{block}
	return &bTreeBranch{node}
}


type BTreeLeaf interface {
	BTreeNode
}

type bTreeLeaf struct {
	*bTreeNode
}

func CreateBTreeLeaf(block *dbio.DataBlock) BTreeLeaf {
	block.Write(BTREE_POS_TYPE, BTREE_TYPE_LEAF)
	node := &bTreeNode{block}
	return &bTreeLeaf{node}
}
