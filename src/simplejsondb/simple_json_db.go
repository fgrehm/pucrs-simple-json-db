package simplejsondb

import (
	"errors"
	log "github.com/Sirupsen/logrus"

	"simplejsondb/actions"
	"simplejsondb/core"
	"simplejsondb/dbio"
)

const BUFFER_SIZE = 256

type MetaDB interface {
	InsertRecord(data string) (uint32, error)
	RemoveRecord(id uint32) error
	FindRecord(id uint32) (*core.Record, error)
	Close() error
}

type metaDb struct {
	dataFile dbio.DataFile
	buffer   dbio.DataBuffer
}

func New(datafilePath string) (MetaDB, error) {
	df, err := dbio.NewDatafile(datafilePath)
	if err != nil {
		return nil, err
	}
	return NewWithDataFile(df)
}

func NewWithDataFile(dataFile dbio.DataFile) (MetaDB, error) {
	dataBuffer := dbio.NewDataBuffer(dataFile, BUFFER_SIZE)
	block, err := dataBuffer.FetchBlock(0)
	if err != nil {
		return nil, err
	}
	if block.ReadUint32(0) == 0 {
		log.Println("Initializing datafile")

		// Next ID = 1
		block.Write(0, uint32(1))
		// Next Available Datablock = 3
		block.Write(4, uint16(3))

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

	record := &core.Record{ID: recordId, Data: data}
	insert := actions.NewRecordAllocator(m.buffer)
	if err = insert.Run(record); err != nil {
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

	rba := core.NewRecordBlockAdapter(block)
	return rba.Remove(rowID.LocalID)
}

func (m *metaDb) FindRecord(id uint32) (*core.Record, error) {
	rowID, err := m.findRowID(id)
	if err != nil {
		return nil, err
	}

	return core.NewRecordFinder(m.buffer).Find(rowID)
}

// HACK: Temporary workaround while we don't have the BTree+ in place
func (m *metaDb) findRowID(needle uint32) (core.RowID, error) {
	block, err := m.buffer.FetchBlock(3)
	if err != nil {
		return core.RowID{}, err
	}

	for {
		rba := core.NewRecordBlockAdapter(block)
		for i, id := range rba.IDs() {
			if id == needle {
				return core.RowID{RecordID: needle, DataBlockID: block.ID, LocalID: uint16(i)}, nil
			}
		}

		nextBlockID := rba.NextBlockID()
		if nextBlockID != 0 {
			block, err = m.buffer.FetchBlock(nextBlockID)
			if err != nil {
				return core.RowID{}, err
			}
		} else {
			return core.RowID{}, errors.New("Not found")
		}
	}
}
