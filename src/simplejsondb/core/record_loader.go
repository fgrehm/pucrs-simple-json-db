package core

import (
	log "github.com/Sirupsen/logrus"
	"simplejsondb/dbio"
)

type RecordLoader interface {
	Load(id uint32, rowID RowID) (*Record, error)
}

type recordLoader struct {
	buffer dbio.DataBuffer
}

func NewRecordLoader(buffer dbio.DataBuffer) RecordLoader {
	return &recordLoader{buffer}
}

func (rf *recordLoader) Load(id uint32, rowID RowID) (*Record, error) {
	repo := NewDataBlockRepository(rf.buffer)
	rb := repo.RecordBlock(rowID.DataBlockID)

	log.Infof("FIND_RECORD recordID=%d, rowID='%d:%d'", id, rowID.DataBlockID, rowID.LocalID)
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
		log.Infof("GET_CHAINED recordID=%d, chainerRowID='%d:%d'", id, chainedRowID.DataBlockID, chainedRowID.LocalID)
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

	return &Record{ID: id, Data: data}, nil
}
