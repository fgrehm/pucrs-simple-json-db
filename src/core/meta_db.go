package core

import (
	"log"
)

const (
	BUFFER_SIZE        = 256
	RECORD_HEADER_SIZE = uint16(12)
)

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
	// TODO: Find out if data fits in a block in advance (chained rows will come later)

	block, err := m.buffer.FetchBlock(0)
	if err != nil {
		return 0, err
	}

	recordId := block.ReadUint32(0)
	// Write back the next ID
	block.Write(0, uint32(recordId+1))
	m.buffer.MarkAsDirty(block.ID)

	record := &Record{ID: recordId, Data: data}
	allocator := newRecordAllocator(m.buffer)
	if err = allocator.Run(record); err != nil {
		return 0, err
	}

	return recordId, nil
}

func (m *metaDb) Close() error {
	if err := m.buffer.Sync(); err != nil {
		return err
	}
	return m.dataFile.Close()
}
