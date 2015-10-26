package core

import (
	"errors"
	"log"
)

const BUFFER_SIZE = 256

type MetaDB interface {
	InsertRecord(data string) (uint32, error)
	RemoveRecord(id uint32) error
	FindRecord(id uint32) (*Record, error)
	Close() error
}

type Record struct {
	ID   uint32
	Data string
}

type RowID struct {
	RecordID    uint32
	DataBlockID uint16
	LocalID     uint16
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

func (m *metaDb) Close() error {
	if err := m.buffer.Sync(); err != nil {
		return err
	}
	return m.dataFile.Close()
}

func (m *metaDb) InsertRecord(data string) (uint32, error) {
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
	// TODO: After inserting the record, need to update the BTree+ index

	return recordId, nil
}

func (m *metaDb) RemoveRecord(id uint32) error {
	rowID, err := m.findRowID(id)
	if err != nil {
		return err
	}

	block, err := m.buffer.FetchBlock(rowID.DataBlockID)
	if err != nil {
		return err
	}

	rba := &recordBlockAdapter{block}
	return rba.Remove(rowID.LocalID)
}

func (m *metaDb) FindRecord(id uint32) (*Record, error) {
	rowID, err := m.findRowID(id)
	if err != nil {
		return nil, err
	}

	return newRecordFinder(m.buffer).Find(rowID)
}

// HACK: Temporary workaround while we don't have the BTree+ in place
func (m *metaDb) findRowID(needle uint32) (RowID, error) {
	// FIXME: Needs to deal with records on a block != 1
	block, err := m.buffer.FetchBlock(1)
	if err != nil {
		return RowID{}, err
	}

	for {
		rba := &recordBlockAdapter{block}
		for i, id := range rba.IDs() {
			if id == needle {
				return RowID{RecordID: needle, DataBlockID: block.ID, LocalID: uint16(i)}, nil
			}
		}

		nextBlockID := rba.NextBlockID()
		if nextBlockID != 0 {
			block, err = m.buffer.FetchBlock(nextBlockID)
			if err != nil {
				return RowID{}, err
			}
		} else {
			return RowID{}, errors.New("Not found")
		}
	}
}
