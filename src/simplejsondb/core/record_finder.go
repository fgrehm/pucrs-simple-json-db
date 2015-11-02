package core

import (
	log "github.com/Sirupsen/logrus"
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
	repo := NewDataBlockRepository(rf.buffer)
	rb := repo.RecordBlock(rowID.DataBlockID)

	log.Infof("FIND_RECORD recordid=%d, rowid='%d:%d'", rowID.RecordID, rowID.DataBlockID, rowID.LocalID)
	data, err := rb.ReadRecordData(rowID.LocalID)
	if err != nil {
		return nil, err
	}
	chainedRowID, err := rb.ChainedRowID(rowID.LocalID)
	if err != nil {
		return nil, err
	}

	for chainedRowID.DataBlockID != 0 {
		rb = repo.RecordBlock(chainedRowID.DataBlockID)
		log.Infof("GET_CHAINED recordid=%d, chainedrowid='%d:%d'", rowID.RecordID, chainedRowID.DataBlockID, chainedRowID.LocalID)
		chainedData, err := rb.ReadRecordData(chainedRowID.LocalID)
		if err != nil {
			return nil, err
		}
		data += chainedData
		chainedRowID, err = rb.ChainedRowID(chainedRowID.LocalID)
		if err != nil {
			return nil, err
		}
	}

	return &Record{ID: rowID.RecordID, Data: data}, nil
}
