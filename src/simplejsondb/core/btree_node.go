package core

import (
	log "github.com/Sirupsen/logrus"
	"simplejsondb/dbio"
)

type BTreeNode interface {
	DataBlockID() uint16
	IsLeaf() bool
	IsRoot() bool
	Parent() uint16
	EntriesCount() uint16
	Reset()
	SetParent(node BTreeNode)
	SetParentID(parentID uint16)
	SetLeftSibling(node BTreeNode)
	LeftSibling() uint16
	SetRightSibling(node BTreeNode)
	SetRightSiblingID(rightID uint16)
	RightSibling() uint16
}

const (
	BTREE_TYPE_BRANCH = uint8(1)
	BTREE_TYPE_LEAF   = uint8(2)

	BTREE_POS_TYPE           = 0
	BTREE_POS_ENTRIES_COUNT  = BTREE_POS_TYPE + 1
	BTREE_POS_PARENT_ID      = BTREE_POS_ENTRIES_COUNT + 2
	BTREE_POS_LEFT_SIBLING   = BTREE_POS_PARENT_ID + 2
	BTREE_POS_RIGHT_SIBLING  = BTREE_POS_LEFT_SIBLING + 2
	BTREE_POS_ENTRIES_OFFSET = BTREE_POS_RIGHT_SIBLING + 2
)

type bTreeNode struct {
	block *dbio.DataBlock
}

func (n *bTreeNode) DataBlockID() uint16 {
	return n.block.ID
}

func (n *bTreeNode) IsLeaf() bool {
	return n.block.ReadUint8(BTREE_POS_TYPE) == BTREE_TYPE_LEAF
}

func (n *bTreeNode) EntriesCount() uint16 {
	return n.block.ReadUint16(BTREE_POS_ENTRIES_COUNT)
}

func (n *bTreeNode) IsRoot() bool {
	return n.block.ReadUint16(BTREE_POS_PARENT_ID) == 0
}

func (n *bTreeNode) SetParent(node BTreeNode) {
	n.SetParentID(node.DataBlockID())
}

func (n *bTreeNode) SetParentID(parent uint16) {
	n.block.Write(BTREE_POS_PARENT_ID, parent)
}

func (n *bTreeNode) Parent() uint16 {
	return n.block.ReadUint16(BTREE_POS_PARENT_ID)
}

func (n *bTreeNode) SetLeftSibling(node BTreeNode) {
	n.block.Write(BTREE_POS_LEFT_SIBLING, node.DataBlockID())
}

func (n *bTreeNode) LeftSibling() uint16 {
	return n.block.ReadUint16(BTREE_POS_LEFT_SIBLING)
}

func (n *bTreeNode) SetRightSibling(node BTreeNode) {
	n.block.Write(BTREE_POS_RIGHT_SIBLING, node.DataBlockID())
}

func (n *bTreeNode) SetRightSiblingID(rightID uint16) {
	n.block.Write(BTREE_POS_RIGHT_SIBLING, rightID)
}

func (n *bTreeNode) RightSibling() uint16 {
	return n.block.ReadUint16(BTREE_POS_RIGHT_SIBLING)
}

func (n *bTreeNode) Reset() {
	log.Printf("RESET blockid=%d", n.block.ID)
	n.block.Write(BTREE_POS_ENTRIES_COUNT, uint16(0))
	n.block.Write(BTREE_POS_PARENT_ID, uint16(0))
	n.block.Write(BTREE_POS_LEFT_SIBLING, uint16(0))
	n.block.Write(BTREE_POS_RIGHT_SIBLING, uint16(0))
}
