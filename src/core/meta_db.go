package core

import (
	"log"
)

const BUFFER_SIZE = 256

type MetaDB interface {
	InsertRecord(data string) (uint64, error)
	Close() error
	// FindRecord(id uint64) (*Record, error)
	// SearchFor(key, value string) ([]*Record, error)
}

type metaDb struct {
	dataFile Datafile
	buffer   DataBuffer
}

func NewMetaDB(datafilePath string) (MetaDB, error) {
	df, err := newDatafile(datafilePath)
	if err != nil {
		return nil, err
	}
	return NewMetaDBWithDataFile(df)
}

func NewMetaDBWithDataFile(dataFile Datafile) (MetaDB, error) {
	dataBuffer := NewDataBuffer(dataFile, BUFFER_SIZE)
	block, err := dataBuffer.FetchBlock(0)
	if err != nil {
		return nil, err
	}
	if DatablockByteOrder.Uint64(block.Data[0:8]) == 0 {
		log.Println("Initializing datafile")

		// Next ID = 1
		DatablockByteOrder.PutUint64(block.Data[0:8], 1)
		// Next Available Datablock = 1
		DatablockByteOrder.PutUint16(block.Data[8:10], 1)

		dataBuffer.MarkAsDirty(0)
		if err = dataBuffer.Sync(); err != nil {
			return nil, err
		}
	}
	return &metaDb{dataFile, dataBuffer}, nil
}

func (m *metaDb) InsertRecord(data string) (uint64, error) {
	// Find out if data fits in a block in advance (chained rows will come later)
	// Find out the next available datablock
	//   Read datablock zero, find out the first block has space available for insertion
	// Assign an ID and increment it (and flag the corresponding datablock that stores the ID as dirty on buffer)

	block, err := m.buffer.FetchBlock(0)
	if err != nil {
		return 0, err
	}

	recordId := DatablockByteOrder.Uint64(block.Data[0:8])
	insertBlockId := DatablockByteOrder.Uint16(block.Data[8:10])
	// Next ID
	DatablockByteOrder.PutUint64(block.Data[0:8], recordId+1)

	block, err = m.buffer.FetchBlock(insertBlockId)
	for index, char := range []byte(data) {
		block.Data[index] = char
	}

	m.buffer.MarkAsDirty(0)
	m.buffer.MarkAsDirty(1)

	return recordId, nil
}

func (m *metaDb) Close() error {
	if err := m.buffer.Sync(); err != nil {
		return err
	}
	return m.dataFile.Close()
}
