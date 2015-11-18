package core

import (
	"simplejsondb/dbio"
)

const (
	POS_NEXT_AVAILABLE_DATABLOCK = 0
	POS_FIRST_BLOCK_PTR          = 2
	POS_BTREE_ROOT               = 4
	POS_BTREE_FIRST_LEAF         = 6
)

type ControlBlock interface {
	DataBlockID() uint16
	Format()
	FirstRecordDataBlock() uint16
	SetFirstRecordDataBlock(dataBlockID uint16)
	NextAvailableRecordsDataBlockID() uint16
	SetNextAvailableRecordsDataBlockID(dataBlockID uint16)
	SetIndexRootBlockID(blockID uint16)
	IndexRootBlockID() uint16
	SetFirstLeaf(blockID uint16)
	FirstLeaf() uint16
}

type controlBlock struct {
	block *dbio.DataBlock
}

func (cb *controlBlock) DataBlockID() uint16 {
	return cb.block.ID
}

func (cb *controlBlock) Format() {
	// Next Available Datablock = 3
	cb.block.Write(POS_NEXT_AVAILABLE_DATABLOCK, uint16(3))
	// Where the linked list starts
	cb.block.Write(POS_FIRST_BLOCK_PTR, uint16(3))
	// Where the BTree index starts
	cb.block.Write(POS_BTREE_ROOT, uint16(0))
	cb.block.Write(POS_BTREE_FIRST_LEAF, uint16(0))
}

func (cb *controlBlock) FirstRecordDataBlock() uint16 {
	return cb.block.ReadUint16(POS_FIRST_BLOCK_PTR)
}

func (cb *controlBlock) SetFirstRecordDataBlock(blockID uint16) {
	cb.block.Write(POS_FIRST_BLOCK_PTR, blockID)
}

func (cb *controlBlock) SetFirstLeaf(blockID uint16) {
	cb.block.Write(POS_BTREE_FIRST_LEAF, blockID)
}

func (cb *controlBlock) FirstLeaf() uint16 {
	return cb.block.ReadUint16(POS_BTREE_FIRST_LEAF)
}

func (cb *controlBlock) SetIndexRootBlockID(blockID uint16) {
	cb.block.Write(POS_BTREE_ROOT, blockID)
}

func (cb *controlBlock) IndexRootBlockID() uint16 {
	return cb.block.ReadUint16(POS_BTREE_ROOT)
}

func (cb *controlBlock) NextAvailableRecordsDataBlockID() uint16 {
	return cb.block.ReadUint16(POS_NEXT_AVAILABLE_DATABLOCK)
}

func (cb *controlBlock) SetNextAvailableRecordsDataBlockID(dataBlockID uint16) {
	cb.block.Write(POS_NEXT_AVAILABLE_DATABLOCK, dataBlockID)
}
