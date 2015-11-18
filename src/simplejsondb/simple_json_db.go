package simplejsondb

import (
	"bytes"
	"encoding/json"

	"simplejsondb/actions"
	"simplejsondb/core"
	"simplejsondb/dbio"
)

const (
	BUFFER_SIZE                  = 256
	BTREE_IDX_BRANCH_MAX_ENTRIES = 680
	BTREE_IDX_LEAF_MAX_ENTRIES   = 510
)

type SimpleJSONDB interface {
	InsertRecord(id uint32, data string) error
	DeleteRecord(id uint32) error
	FindRecord(id uint32) (*core.Record, error)
	UpdateRecord(id uint32, data string) error
	Close() error
}

type simpleJSONDB struct {
	dataFile dbio.DataFile
	buffer   dbio.DataBuffer
	repo     core.DataBlockRepository
	index    core.Uint32Index
}

func New(datafilePath string) (SimpleJSONDB, error) {
	df, err := dbio.NewDatafile(datafilePath)
	if err != nil {
		return nil, err
	}
	return NewWithDataFile(df)
}

func NewWithDataFile(dataFile dbio.DataFile) (SimpleJSONDB, error) {
	if err := core.FormatDataFileIfNeeded(dataFile); err != nil {
		return nil, err
	}

	dataBuffer := dbio.NewDataBuffer(dataFile, BUFFER_SIZE)
	repo := core.NewDataBlockRepository(dataBuffer)
	index := core.NewUint32Index(dataBuffer, BTREE_IDX_BRANCH_MAX_ENTRIES, BTREE_IDX_LEAF_MAX_ENTRIES)
	return &simpleJSONDB{dataFile, dataBuffer, repo, index}, nil
}

func (db *simpleJSONDB) Close() error {
	if err := db.buffer.Sync(); err != nil {
		return err
	}
	return db.dataFile.Close()
}

func (db *simpleJSONDB) InsertRecord(id uint32, data string) error {
	var jsonBuffer bytes.Buffer
	if err := json.Compact(&jsonBuffer, []byte(data)); err != nil {
		return err
	}
	record := &core.Record{ID: id, Data: jsonBuffer.Bytes()}
	return actions.Insert(db.index, db.buffer, record)
}

func (db *simpleJSONDB) UpdateRecord(id uint32, data string) error {
	var jsonBuffer bytes.Buffer
	if err := json.Compact(&jsonBuffer, []byte(data)); err != nil {
		return err
	}
	record := &core.Record{ID: id, Data: jsonBuffer.Bytes()}
	return actions.Update(db.index, db.buffer, record)
}

func (db *simpleJSONDB) DeleteRecord(id uint32) error {
	return actions.Delete(db.index, db.buffer, id)
}

func (db *simpleJSONDB) FindRecord(id uint32) (*core.Record, error) {
	return actions.Find(db.index, db.buffer, id)
}
