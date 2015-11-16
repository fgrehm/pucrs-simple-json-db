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
	InsertRecord(id uint32, data string) error
	RemoveRecord(id uint32) error
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

// TODO: Delegate to an insert action
func (db *simpleJSONDB) InsertRecord(id uint32, data string) error {
	cb := core.NewDataBlockRepository(db.buffer).ControlBlock()
	db.buffer.MarkAsDirty(cb.DataBlockID())

	record := &core.Record{ID: id, Data: data}
	allocator := actions.NewRecordAllocator(db.buffer)
	if _, err := allocator.Add(record); err != nil {
		return err
	}
	// TODO: After inserting the record, need to update the BTree+ index

	return nil
}

// TODO: Delegate to an update action
func (db *simpleJSONDB) UpdateRecord(recordID uint32, data string) error {
	rowID, err := db.findRowID(recordID)
	if err != nil {
		return err
	}

	record := &core.Record{ID: recordID, Data: data}
	allocator := actions.NewRecordAllocator(db.buffer)
	if err = allocator.Update(rowID, record); err != nil {
		return err
	}

	return nil
}

// TODO: Delegate to a remove action
func (db *simpleJSONDB) RemoveRecord(id uint32) error {
	rowID, err := db.findRowID(id)
	if err != nil {
		return err
	}

	allocator := actions.NewRecordAllocator(db.buffer)
	return allocator.Remove(rowID)
}

func (db *simpleJSONDB) FindRecord(id uint32) (*core.Record, error) {
	rowID, err := db.findRowID(id)
	if err != nil {
		return nil, err
	}

	return core.NewRecordFinder(db.buffer).Find(id, rowID)
}

// HACK: Temporary workaround while we don't have the BTree+ in place
func (db *simpleJSONDB) findRowID(needle uint32) (core.RowID, error) {
	log.Debugf("Looking up the RowID for %d", needle)
	repo := core.NewDataBlockRepository(db.buffer)

	blockID := repo.ControlBlock().FirstRecordDataBlock()
	for {
		rb := repo.RecordBlock(blockID)
		for i, id := range rb.IDs() {
			if id == needle {
				return core.RowID{DataBlockID: blockID, LocalID: uint16(i)}, nil
			}
		}

		blockID = rb.NextBlockID()
		log.Debugf("Reading the next block %d", blockID)
		if blockID == 0 {
			return core.RowID{}, errors.New("Not found")
		}
	}
}
