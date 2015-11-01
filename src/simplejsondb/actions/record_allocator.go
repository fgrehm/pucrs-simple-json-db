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
}

func NewRecordAllocator(buffer dbio.DataBuffer) RecordAllocator {
	return &recordAllocator{buffer}
}

func (ra *recordAllocator) Add(record *core.Record) (core.RowID, error) {
	log.Printf("INSERT recordid=%d", record.ID)

	controlDataBlock, err := ra.buffer.FetchBlock(0)
	if err != nil {
		return core.RowID{}, err
	}

	controlBlock := core.NewControlBlock(controlDataBlock)
	insertBlockID := controlBlock.NextAvailableRecordsDataBlockID()
	var localID uint16

	for {
		block, err := ra.buffer.FetchBlock(insertBlockID)
		if err != nil {
			return core.RowID{}, err
		}
		recordBlock := core.NewRecordBlock(block)
		freeSpaceForInsert := recordBlock.FreeSpaceForInsert()

		// XXX: This is not ideal since it may result in having a single byte on a
		//      datablock for a chained row
		if freeSpaceForInsert > 0 {
			localID, err = ra.allocateRecord(int(freeSpaceForInsert), block, record)
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

func (ra *recordAllocator) allocateRecord(freeSpaceForInsert int, initialBlock *dbio.DataBlock, record *core.Record) (uint16, error) {
	dataToWrite := []byte(record.Data)
	bytesToWrite := len(dataToWrite)
	recordBlock := core.NewRecordBlock(initialBlock)

	// Does the record fit on the datablock?
	if len(dataToWrite) > freeSpaceForInsert {
		bytesToWrite = freeSpaceForInsert
	}

	// Write as many bytes as we can
	recordLocalID := recordBlock.Add(record.ID, dataToWrite[0:bytesToWrite])
	dataToWrite = dataToWrite[bytesToWrite:]

	currBlockID := initialBlock.ID
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
		nextDataBlockCandidate, err := ra.buffer.FetchBlock(nextBlockID)
		if err != nil {
			return 0, err
		}
		nextBlock := core.NewRecordBlock(nextDataBlockCandidate)
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
	blocksMap := core.NewDataBlocksMap(ra.buffer)
	newBlockID := blocksMap.FirstFree()
	blocksMap.MarkAsUsed(newBlockID)
	log.Printf("ALLOCATE blockid=%d, prevblockid=%d", newBlockID, startingBlockID)

	block, err := ra.buffer.FetchBlock(newBlockID)
	if err != nil {
		return 0, err
	}
	core.NewRecordBlock(block).SetPrevBlockID(startingBlockID)

	block, err = ra.buffer.FetchBlock(0)
	if err != nil {
		return 0, err
	}
	controlBlock := core.NewControlBlock(block)
	controlBlock.SetNextAvailableRecordsDataBlockID(newBlockID)
	ra.buffer.MarkAsDirty(block.ID)

	return newBlockID, nil
}

func (ra *recordAllocator) Remove(rowID core.RowID) error {
	block, err := ra.buffer.FetchBlock(rowID.DataBlockID)
	if err != nil {
		return err
	}

	initialRecordBlock := core.NewRecordBlock(block)

	chainedRowID, err := initialRecordBlock.ChainedRowID(rowID.LocalID)
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
	if err = initialRecordBlock.Remove(rowID.LocalID); err != nil {
		return err
	}

	if initialRecordBlock.TotalRecords() == 0 {
		log.Printf("FREE blockid=%d, prevblockid=%d, nextblockid=%d", block.ID, initialRecordBlock.PrevBlockID(), initialRecordBlock.NextBlockID())
		if err := ra.removeFromList(block); err != nil {
			return err
		}
	}

	return nil
}

func (ra *recordAllocator) removeFromList(emptyDataBlock *dbio.DataBlock) error {
	emptyBlock := core.NewRecordBlock(emptyDataBlock)
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
		controlBlock := core.NewControlBlock(controlDataBlock)
		controlBlock.SetFirstRecordDataBlock(nextBlockID)
		ra.buffer.MarkAsDirty(controlDataBlock.ID)
	}

	// Last block on the list
	if nextBlockID == 0 {
		return nil
	}

	// General case, set the next block pointer of the previous entry to the one after the block being deleted
	prevDataBlock, err := ra.buffer.FetchBlock(prevBlockID)
	if err != nil {
		return err
	}
	prevBlock := core.NewRecordBlock(prevDataBlock)
	prevBlock.SetNextBlockID(nextBlockID)
	ra.buffer.MarkAsDirty(prevBlockID)

	// And point the next block to the one before this one
	nextDataBlock, err := ra.buffer.FetchBlock(nextBlockID)
	if err != nil {
		return err
	}
	nextBlock := core.NewRecordBlock(nextDataBlock)
	nextBlock.SetPrevBlockID(prevBlockID)
	ra.buffer.MarkAsDirty(nextBlockID)

	// Clear out headers
	emptyBlock.Clear()

	// Get the block back into the pool of free blocks
	blocksMap := core.NewDataBlocksMap(ra.buffer)
	blocksMap.MarkAsFree(emptyDataBlock.ID)

	return nil
}

func (ra *recordAllocator) Update(rowID core.RowID, newData string) error {
	log.Infof("UPDATE recordid=%d, rowid='%d:%d'", rowID.RecordID, rowID.DataBlockID, rowID.LocalID)

	block, err := ra.buffer.FetchBlock(rowID.DataBlockID)
	if err != nil {
		return err
	}

	rb := core.NewRecordBlock(block)
	chainedID, err := rb.ChainedRowID(rowID.LocalID)
	if err != nil {
		return err
	}
	if chainedID.DataBlockID == 0 {
		ra.Remove(chainedID)
	}

	if err = rb.SoftRemove(rowID.LocalID); err != nil {
		return err
	}

	localID := rb.Add(rowID.RecordID, []byte(newData))
	if localID != rowID.LocalID {
		panic(fmt.Sprintf("Something weird happened while updating the record, its local ID changed from %d to %d", rowID.LocalID, localID))
	}

	return nil
}
