package core

import (
	"simplejsondb/dbio"
)

type RecordFinder interface {
	Find(rowID RowID) (*Record, error)
}

type recordFinder struct {
	buffer dbio.DataBuffer
}

func NewRecordFinder(buffer dbio.DataBuffer) RecordFinder {
	return &recordFinder{buffer}
}

func (rf *recordFinder) Find(rowID RowID) (*Record, error) {
	block, err := rf.buffer.FetchBlock(rowID.DataBlockID)
	if err != nil {
		return nil, err
	}
	rba := NewRecordBlock(block)

	// TODO: Deal with chained rows, BTree and the like
	data, err := rba.ReadRecordData(rowID.LocalID)
	if err != nil {
		return nil, err
	}

	return &Record{ID: rowID.RecordID, Data: data}, nil
}
