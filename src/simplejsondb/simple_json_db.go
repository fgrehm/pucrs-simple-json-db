package simplejsondb

import (
	"errors"
	log "github.com/Sirupsen/logrus"

	"simplejsondb/actions"
	"simplejsondb/core"
	"simplejsondb/dbio"
)

const BUFFER_SIZE = 256

type SimpleJSONDB interface {
	InsertRecord(data string) (uint32, error)
	RemoveRecord(id uint32) error
	FindRecord(id uint32) (*core.Record, error)
	Close() error
}

type simpleJSONDB struct {
	dataFile dbio.DataFile
	buffer   dbio.DataBuffer
}

func New(datafilePath string) (SimpleJSONDB, error) {
	df, err := dbio.NewDatafile(datafilePath)
	if err != nil {
		return nil, err
	}
	return NewWithDataFile(df)
}

func NewWithDataFile(dataFile dbio.DataFile) (SimpleJSONDB, error) {
	dataBuffer := dbio.NewDataBuffer(dataFile, BUFFER_SIZE)
	block, err := dataBuffer.FetchBlock(0)
	if err != nil {
		return nil, err
	}
	jsonDB := &simpleJSONDB{dataFile, dataBuffer}

	if block.ReadUint32(0) == 0 {
		if err := jsonDB.format(block); err != nil {
			return nil, err
		}
	}
	return jsonDB, nil
}

func (m *simpleJSONDB) Close() error {
	if err := m.buffer.Sync(); err != nil {
		return err
	}
	return m.dataFile.Close()
}

func (m *simpleJSONDB) InsertRecord(data string) (uint32, error) {
	block, err := m.buffer.FetchBlock(0)
	if err != nil {
		return 0, err
	}

	cb := core.NewControlBlock(block)
	recordId := cb.NextID()
	cb.IncNextID()
	m.buffer.MarkAsDirty(block.ID)

	record := &core.Record{ID: recordId, Data: data}
	insert := actions.NewRecordAllocator(m.buffer)
	if err = insert.Run(record); err != nil {
		return 0, err
	}
	// TODO: After inserting the record, need to update the BTree+ index

	return recordId, nil
}

func (m *simpleJSONDB) RemoveRecord(id uint32) error {
	rowID, err := m.findRowID(id)
	if err != nil {
		return err
	}

	// TODO: Extract to a separate object and deal with chained rows
	block, err := m.buffer.FetchBlock(rowID.DataBlockID)
	if err != nil {
		return err
	}

	rba := core.NewRecordBlock(block)
	return rba.Remove(rowID.LocalID)
}

func (m *simpleJSONDB) FindRecord(id uint32) (*core.Record, error) {
	rowID, err := m.findRowID(id)
	if err != nil {
		return nil, err
	}

	return core.NewRecordFinder(m.buffer).Find(rowID)
}

func (db *simpleJSONDB) format(blockZero *dbio.DataBlock) error {
	log.Println("Initializing datafile")

	// Next ID = 1
	blockZero.Write(core.POS_NEXT_ID, uint32(1))
	// Next Available Datablock = 3
	blockZero.Write(core.POS_NEXT_AVAILABLE_DATABLOCK, uint16(3))
	db.buffer.MarkAsDirty(blockZero.ID)

	blockMap := core.NewDataBlocksMap(db.buffer)

	for i := uint16(0); i < 4; i++ {
		blockMap.MarkAsUsed(i)
	}

	if err := db.buffer.Sync(); err != nil {
		return err
	}

	return nil
}

// HACK: Temporary workaround while we don't have the BTree+ in place
func (m *simpleJSONDB) findRowID(needle uint32) (core.RowID, error) {
	block, err := m.buffer.FetchBlock(3)
	if err != nil {
		return core.RowID{}, err
	}

	for {
		rba := core.NewRecordBlock(block)
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
