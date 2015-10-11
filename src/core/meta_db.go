package core

import (
	"log"
)

const BUFFER_SIZE = 256

type MetaDB interface {
	InsertRecord(data string) (uint32, error)
	Close() error
	// FindRecord(id uint64) (*Record, error)
	// SearchFor(key, value string) (<-chan Record, error)
}

type metaDb struct {
	dataFile DataFile
	buffer   DataBuffer
}

func NewMetaDB(datafilePath string) (MetaDB, error) {
	df, err := newDatafile(datafilePath)
	if err != nil {
		return nil, err
	}
	return NewMetaDBWithDataFile(df)
}

func NewMetaDBWithDataFile(dataFile DataFile) (MetaDB, error) {
	dataBuffer := NewDataBuffer(dataFile, BUFFER_SIZE)
	block, err := dataBuffer.FetchBlock(0)
	if err != nil {
		return nil, err
	}
	if block.ReadUint32(0) == 0 {
		log.Println("Initializing datafile")

		// Next ID = 1
		block.Write(0, uint32(1))
		// Next Available Datablock = 1
		block.Write(4, uint16(1))

		dataBuffer.MarkAsDirty(block.ID)
		if err = dataBuffer.Sync(); err != nil {
			return nil, err
		}
	}
	return &metaDb{dataFile, dataBuffer}, nil
}

func (m *metaDb) InsertRecord(data string) (uint32, error) {
	// Find out if data fits in a block in advance (chained rows will come later)
	// Find out the next available datablock
	//   Read datablock zero, find out the first block has space available for insertion
	// Assign an ID and increment it (and flag the corresponding datablock that stores the ID as dirty on buffer)

	block, err := m.buffer.FetchBlock(0)
	if err != nil {
		return 0, err
	}

	recordId := block.ReadUint32(0)
	insertBlockId := block.ReadUint16(4)
	// Next ID
	block.Write(0, recordId+1)
	m.buffer.MarkAsDirty(block.ID)

	block, err = m.buffer.FetchBlock(insertBlockId)
	block.Write(0, data)
	m.buffer.MarkAsDirty(block.ID)

	return recordId, nil
}

func (m *metaDb) Close() error {
	if err := m.buffer.Sync(); err != nil {
		return err
	}
	return m.dataFile.Close()
}
