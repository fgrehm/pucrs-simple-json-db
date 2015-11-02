package actions

import (
	"fmt"
	log "github.com/Sirupsen/logrus"

	"simplejsondb/core"
	"simplejsondb/dbio"
)

type RecordAllocator interface {
	Add(record *core.Record) (core.RowID, error)
	Update(rowID core.RowID, newData string) error
	Remove(rowID core.RowID) error
}

type recordAllocator struct {
	buffer dbio.DataBuffer
	repo   core.DataBlockRepository
}

func NewRecordAllocator(buffer dbio.DataBuffer) RecordAllocator {
	repo := core.NewDataBlockRepository(buffer)
	return &recordAllocator{buffer, repo}
}

func (ra *recordAllocator) Add(record *core.Record) (core.RowID, error) {
	log.Printf("INSERT recordid=%d", record.ID)

	controlBlock := ra.repo.ControlBlock()
	insertBlockID := controlBlock.NextAvailableRecordsDataBlockID()

	var localID uint16
	var err error

	for {
		recordBlock := ra.repo.RecordBlock(insertBlockID)
		freeSpaceForInsert := recordBlock.FreeSpaceForInsert()

		// XXX: This is not ideal since it may result in having a single byte on a
		//      datablock for a chained row
		if freeSpaceForInsert > 0 {
			localID, err = ra.allocateRecord(int(freeSpaceForInsert), recordBlock, record)
			if err != nil {
				return core.RowID{}, err
			}
			break
		}

		// Does not fit on this datablock, move on to the next one
		if nextID := recordBlock.NextBlockID(); nextID != 0 {
			insertBlockID = nextID
			continue
		}

		newBlockID, err := ra.allocateNewBlock(insertBlockID)
		if err != nil {
			return core.RowID{}, err
		}
		recordBlock.SetNextBlockID(newBlockID)
		ra.buffer.MarkAsDirty(insertBlockID)

		insertBlockID = newBlockID
	}

	ra.buffer.MarkAsDirty(insertBlockID)
	return core.RowID{DataBlockID: insertBlockID, LocalID: localID}, nil
}

func (ra *recordAllocator) allocateRecord(freeSpaceForInsert int, initialBlock core.RecordBlock, record *core.Record) (uint16, error) {
	dataToWrite := []byte(record.Data)
	bytesToWrite := len(dataToWrite)
	recordBlock := initialBlock

	// Does the record fit on the datablock?
	if len(dataToWrite) > freeSpaceForInsert {
		bytesToWrite = freeSpaceForInsert
	}

	// Write as many bytes as we can
	recordLocalID := recordBlock.Add(record.ID, dataToWrite[0:bytesToWrite])
	dataToWrite = dataToWrite[bytesToWrite:]

	currBlockID := initialBlock.DataBlockID()
	currRecordBlock := recordBlock
	currLocalID := recordLocalID
	nextBlockID := recordBlock.NextBlockID()
	var err error

	for bytesToWrite = len(dataToWrite); bytesToWrite > 0; bytesToWrite = len(dataToWrite) {
		log.Debugf("Remaining data `% x`", dataToWrite)

		// Do we need a new block to add to the end of the list?
		if nextBlockID == 0 {
			nextBlockID, err = ra.allocateNewBlock(currBlockID)
			if err != nil {
				return 0, err
			}
			currRecordBlock.SetNextBlockID(nextBlockID)
			ra.buffer.MarkAsDirty(currBlockID)
		}

		// Can we write something to the next data block?
		nextBlock := ra.repo.RecordBlock(nextBlockID)
		freeSpaceForInsertOnNewBlock := int(nextBlock.FreeSpaceForInsert())

		// The block is full, move on on the linked list
		if freeSpaceForInsertOnNewBlock == 0 {
			currBlockID = nextBlockID
			nextBlockID = nextBlock.NextBlockID()
			continue
		}

		// At this point we can write something on the block, but does it fit on a single block?
		if bytesToWrite > freeSpaceForInsertOnNewBlock {
			bytesToWrite = freeSpaceForInsertOnNewBlock
		}

		log.Debugf("Bytes to write %d", bytesToWrite)

		// Write data and set up the chained row stuff
		chainedLocalID := nextBlock.Add(record.ID, dataToWrite[0:bytesToWrite])
		chainedRowID := core.RowID{DataBlockID: nextBlockID, LocalID: chainedLocalID}
		currRecordBlock.SetChainedRowID(currLocalID, chainedRowID)
		ra.buffer.MarkAsDirty(currBlockID)
		ra.buffer.MarkAsDirty(nextBlockID)

		// Continue writing on next blocks
		currLocalID = chainedLocalID
		currBlockID = nextBlockID
		currRecordBlock = nextBlock
		nextBlockID = nextBlock.NextBlockID()
		dataToWrite = dataToWrite[bytesToWrite:]
	}

	return recordLocalID, nil
}

func (ra *recordAllocator) allocateNewBlock(startingBlockID uint16) (uint16, error) {
	blocksMap := ra.repo.DataBlocksMap()
	newBlockID := blocksMap.FirstFree()
	blocksMap.MarkAsUsed(newBlockID)

	log.Printf("ALLOCATE blockid=%d, prevblockid=%d", newBlockID, startingBlockID)

	ra.repo.RecordBlock(newBlockID).SetPrevBlockID(startingBlockID)
	ra.buffer.MarkAsDirty(newBlockID)

	controlBlock := ra.repo.ControlBlock()
	controlBlock.SetNextAvailableRecordsDataBlockID(newBlockID)
	ra.buffer.MarkAsDirty(controlBlock.DataBlockID())

	return newBlockID, nil
}

func (ra *recordAllocator) Remove(rowID core.RowID) error {
	firstBlock := ra.repo.RecordBlock(rowID.DataBlockID)

	chainedRowID, err := firstBlock.ChainedRowID(rowID.LocalID)
	if err != nil {
		return err
	}

	// Remove chained rows first
	if chainedRowID.DataBlockID != 0 {
		if err := ra.Remove(chainedRowID); err != nil {
			return err
		}
	}

	// Then remove the first block that makes up for the record
	if err = firstBlock.Remove(rowID.LocalID); err != nil {
		return err
	}

	if firstBlock.TotalRecords() == 0 {
		log.Printf("FREE blockid=%d, prevblockid=%d, nextblockid=%d", firstBlock.DataBlockID(), firstBlock.PrevBlockID(), firstBlock.NextBlockID())
		if err := ra.removeFromList(firstBlock); err != nil {
			return err
		}
	}

	return nil
}

func (ra *recordAllocator) removeFromList(emptyBlock core.RecordBlock) error {
	prevBlockID := emptyBlock.PrevBlockID()
	nextBlockID := emptyBlock.NextBlockID()

	// First block on the list
	if prevBlockID == 0 {
		// If this is the first and only block on the list, there's nothing to be done
		if nextBlockID == 0 {
			return nil
		}

		// Set the first block to be the one following this one
		controlDataBlock, err := ra.buffer.FetchBlock(0)
		if err != nil {
			return err
		}
		controlBlock := core.NewDataBlockRepository(ra.buffer).ControlBlock()
		controlBlock.SetFirstRecordDataBlock(nextBlockID)
		ra.buffer.MarkAsDirty(controlDataBlock.ID)
	}

	// Last block on the list
	if nextBlockID == 0 {
		return nil
	}

	// General case, set the next block pointer of the previous entry to the one after the block being deleted
	prevBlock := ra.repo.RecordBlock(prevBlockID)
	prevBlock.SetNextBlockID(nextBlockID)
	ra.buffer.MarkAsDirty(prevBlockID)

	// And point the next block to the one before this one
	nextBlock := ra.repo.RecordBlock(nextBlockID)
	nextBlock.SetPrevBlockID(prevBlockID)
	ra.buffer.MarkAsDirty(nextBlockID)

	// Clear out headers
	emptyBlock.Clear()

	// Get the block back into the pool of free blocks
	blocksMap := ra.repo.DataBlocksMap()
	blocksMap.MarkAsFree(emptyBlock.DataBlockID())

	return nil
}

func (ra *recordAllocator) Update(rowID core.RowID, newData string) error {
	log.Infof("UPDATE recordid=%d, rowid='%d:%d'", rowID.RecordID, rowID.DataBlockID, rowID.LocalID)

	rb := ra.repo.RecordBlock(rowID.DataBlockID)
	chainedID, err := rb.ChainedRowID(rowID.LocalID)
	if err != nil {
		return err
	}
	if chainedID.DataBlockID != 0 {
		ra.Remove(chainedID)
	}

	if err = rb.SoftRemove(rowID.LocalID); err != nil {
		return err
	}

	localID := rb.Add(rowID.RecordID, []byte(newData))
	if localID != rowID.LocalID {
		panic(fmt.Sprintf("Something weird happened while updating the record, its local ID changed from %d to %d", rowID.LocalID, localID))
	}
	ra.buffer.MarkAsDirty(rowID.DataBlockID)

	return nil
}
