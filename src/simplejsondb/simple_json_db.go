package simplejsondb

import (
	log "github.com/Sirupsen/logrus"

	"simplejsondb/actions"
	"simplejsondb/core"
	"simplejsondb/dbio"
)

const BUFFER_SIZE = 256

type SimpleJSONDB interface {
	InsertRecord(id uint32, data string) error
	DeleteRecord(id uint32) error
	FindRecord(id uint32) (*core.Record, error)
	UpdateRecord(id uint32, data string) error
	Close() error
}

type simpleJSONDB struct {
	dataFile dbio.DataFile
	repo     core.DataBlockRepository
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
	repo := core.NewDataBlockRepository(dataBuffer)
	jsonDB := &simpleJSONDB{dataFile, repo, dataBuffer}

	controlBlock := repo.ControlBlock()
	if controlBlock.NextAvailableRecordsDataBlockID() == 0 {
		log.Println("FORMAT_DB")
		controlBlock.Format()
		dataBuffer.MarkAsDirty(controlBlock.DataBlockID())

		// rootBlock, err := dataBuffer.FetchBlock(controlBlock.BTreeRootBlock())
		// if err != nil {
		// 	return nil, err
		// }
		// indexRoot := core.CreateBTreeLeaf(rootBlock)
		// dataBuffer.MarkAsDirty(indexRoot.DataBlockID())

		blockMap := repo.DataBlocksMap()
		// REFACTOR: This 5 should be calculated somehow
		for i := uint16(0); i < 5; i++ {
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

func (db *simpleJSONDB) InsertRecord(id uint32, data string) error {
	record := &core.Record{ID: id, Data: data}
	return actions.Insert(db.buffer, record)
}

func (db *simpleJSONDB) UpdateRecord(id uint32, data string) error {
	record := &core.Record{ID: id, Data: data}
	return actions.Update(db.buffer, record)
}

func (db *simpleJSONDB) DeleteRecord(id uint32) error {
	return actions.Delete(db.buffer, id)
}

func (db *simpleJSONDB) FindRecord(id uint32) (*core.Record, error) {
	return actions.Find(db.buffer, id)
}
