package actions

import (
	"fmt"
	log "github.com/Sirupsen/logrus"

	"simplejsondb/core"
	"simplejsondb/dbio"
)

type RecordAllocator interface {
	Add(record *core.Record) (core.RowID, error)
	Update(rowID core.RowID, record *core.Record) error
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
			localID, err = ra.allocateRecord(int(freeSpaceForInsert), recordBlock, record.ID, []byte(record.Data))
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
	return core.RowID{
		RecordID:    record.ID,
		DataBlockID: insertBlockID,
		LocalID:     localID,
	}, nil
}

func (ra *recordAllocator) allocateRecord(freeSpaceForInsert int, initialBlock core.RecordBlock, recordID uint32, data []byte) (uint16, error) {
	bytesToWrite := len(data)

	// Does the record fit on the datablock?
	if bytesToWrite > freeSpaceForInsert {
		bytesToWrite = freeSpaceForInsert
	}

	// Write as many bytes as we can on the initial datablock
	localID := initialBlock.Add(recordID, data[0:bytesToWrite])
	ra.buffer.MarkAsDirty(initialBlock.DataBlockID())

	// If we wrote the whole record, we are done
	if bytesToWrite == len(data) {
		return localID, nil
	}

	// Otherwise we move on to the next block that has free space and continue writing from there
	data = data[bytesToWrite:]
	recordBlock := initialBlock
	prevBlock := recordBlock
	prevLocalID := localID
	for bytesToWrite = len(data); bytesToWrite > 0; bytesToWrite = len(data) {
		for recordBlock.FreeSpaceForInsert() == 0 {
			var err error
			nextBlockID := recordBlock.NextBlockID()
			if nextBlockID == 0 {
				nextBlockID, err = ra.allocateNewBlock(recordBlock.DataBlockID())
				if err != nil {
					return 0, err
				}
			}
			recordBlock = ra.repo.RecordBlock(nextBlockID)
		}

		// Write as much data as we can
		freeSpaceForInsert = int(recordBlock.FreeSpaceForInsert())
		if bytesToWrite > freeSpaceForInsert {
			bytesToWrite = freeSpaceForInsert
		}
		nextLocalID := recordBlock.Add(recordID, data[0:bytesToWrite])
		ra.buffer.MarkAsDirty(recordBlock.DataBlockID())
		nextRowID := core.RowID{RecordID: recordID, DataBlockID: recordBlock.DataBlockID(), LocalID: nextLocalID}

		// And wire up the chain
		prevBlock.SetChainedRowID(prevLocalID, nextRowID)
		ra.buffer.MarkAsDirty(prevBlock.DataBlockID())

		// "Consume" the chunk of bytes
		data = data[bytesToWrite:]

		// Keep track of the last block used on the chain
		prevBlock = recordBlock
		prevLocalID = nextLocalID
	}

	return localID, nil
}

func (ra *recordAllocator) allocateNewBlock(startingBlockID uint16) (uint16, error) {
	blocksMap := ra.repo.DataBlocksMap()
	newBlockID := blocksMap.FirstFree()
	blocksMap.MarkAsUsed(newBlockID)

	log.Printf("ALLOCATE blockid=%d, prevblockid=%d", newBlockID, startingBlockID)

	ra.repo.RecordBlock(newBlockID).SetPrevBlockID(startingBlockID)
	ra.buffer.MarkAsDirty(newBlockID)

	ra.repo.RecordBlock(startingBlockID).SetNextBlockID(newBlockID)
	ra.buffer.MarkAsDirty(startingBlockID)

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

func (ra *recordAllocator) Update(rowID core.RowID, record *core.Record) error {
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

	freeSpaceForInsert := rb.FreeSpaceForInsert()
	localID, err := ra.allocateRecord(int(freeSpaceForInsert), rb, record.ID, []byte(record.Data))
	if rowID.LocalID != localID {
		panic(fmt.Sprintf("Something weird happened while updating the record, its local ID changed from %+v to %+v", rowID.LocalID, localID))
	}

	return err
}
