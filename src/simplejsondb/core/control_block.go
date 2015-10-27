package core

import (
	"simplejsondb/dbio"
)

const (
	POS_NEXT_ID                  = 0
	POS_NEXT_AVAILABLE_DATABLOCK = 4
)

type ControlBlock interface {
	NextID() uint32
	IncNextID()
	NextAvailableRecordsDataBlockID() uint16
	SetNextAvailableRecordsDataBlockID(dataBlockID uint16)
}

type controlBlock struct {
	block *dbio.DataBlock
}

func NewControlBlock(block *dbio.DataBlock) ControlBlock {
	return &controlBlock{block}
}

func (cb *controlBlock) NextID() uint32 {
	return cb.block.ReadUint32(POS_NEXT_ID)
}

func (cb *controlBlock) IncNextID() {
	cb.block.Write(POS_NEXT_ID, cb.NextID()+1)
}

func (cb *controlBlock) NextAvailableRecordsDataBlockID() uint16 {
	return cb.block.ReadUint16(POS_NEXT_AVAILABLE_DATABLOCK)
}

func (cb *controlBlock) SetNextAvailableRecordsDataBlockID(dataBlockID uint16) {
	cb.block.Write(POS_NEXT_AVAILABLE_DATABLOCK, dataBlockID)
}
