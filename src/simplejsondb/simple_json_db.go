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
	blockZero, err := dataBuffer.FetchBlock(0)
	if err != nil {
		return nil, err
	}
	jsonDB := &simpleJSONDB{dataFile, dataBuffer}

	controlBlock := core.NewControlBlock(blockZero)
	if controlBlock.NextID() == 0 {
		log.Println("FORMAT_DB")

		controlBlock.Format()
		dataBuffer.MarkAsDirty(blockZero.ID)

		blockMap := core.NewDataBlocksMap(dataBuffer)
		for i := uint16(0); i < 4; i++ {
			blockMap.MarkAsUsed(i)
		}

		if err := dataBuffer.Sync(); err != nil {
			return nil, err
		}
	}
	return jsonDB, nil
}

func (db *simpleJSONDB) Close() error {
	if err := db.buffer.Sync(); err != nil {
		return err
	}
	return db.dataFile.Close()
}

func (db *simpleJSONDB) InsertRecord(data string) (uint32, error) {
	block, err := db.buffer.FetchBlock(0)
	if err != nil {
		return 0, err
	}

	cb := core.NewControlBlock(block)
	recordId := cb.NextID()
	cb.IncNextID()
	db.buffer.MarkAsDirty(block.ID)

	record := &core.Record{ID: recordId, Data: data}
	insert := actions.NewRecordAllocator(db.buffer)
	if err = insert.Run(record); err != nil {
		return 0, err
	}
	// TODO: After inserting the record, need to update the BTree+ index

	return recordId, nil
}

func (db *simpleJSONDB) RemoveRecord(id uint32) error {
	rowID, err := db.findRowID(id)
	if err != nil {
		return err
	}

	// TODO: Extract to a separate object and deal with chained rows
	block, err := db.buffer.FetchBlock(rowID.DataBlockID)
	if err != nil {
		return err
	}

	rba := core.NewRecordBlock(block)
	return rba.Remove(rowID.LocalID)
}

func (db *simpleJSONDB) FindRecord(id uint32) (*core.Record, error) {
	rowID, err := db.findRowID(id)
	if err != nil {
		return nil, err
	}

	return core.NewRecordFinder(db.buffer).Find(rowID)
}

// HACK: Temporary workaround while we don't have the BTree+ in place
func (db *simpleJSONDB) findRowID(needle uint32) (core.RowID, error) {
	block, err := db.buffer.FetchBlock(3)
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
			block, err = db.buffer.FetchBlock(nextBlockID)
			if err != nil {
				return core.RowID{}, err
			}
		} else {
			return core.RowID{}, errors.New("Not found")
		}
	}
}
