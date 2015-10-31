package actions

import (
	log "github.com/Sirupsen/logrus"

	"simplejsondb/core"
	"simplejsondb/dbio"
)

type RecordAllocator interface {
	Add(record *core.Record) (core.RowID, error)
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
	var rowID core.RowID

	for {
		block, err := ra.buffer.FetchBlock(insertBlockID)
		if err != nil {
			return core.RowID{}, err
		}
		recordBlock := core.NewRecordBlock(block)
		recordLength := len(record.Data)
		freeSpaceForInsert := recordBlock.FreeSpaceForInsert()

		// Does the record fit on the datablock?
		if (int(freeSpaceForInsert) - recordLength) >= 0 {
			localID := recordBlock.Add(record.ID, []byte(record.Data[0:recordLength]))
			rowID = core.RowID{DataBlockID: block.ID, LocalID: localID}
			break
		}

		// Can we "slice" the record and make it a chained row?
		// if freeSpaceForInsert > 0
		//   Find a free contiguous block for insert / update

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

		controlBlock.SetNextAvailableRecordsDataBlockID(newBlockID)
		ra.buffer.MarkAsDirty(controlDataBlock.ID)

		insertBlockID = newBlockID
	}

	ra.buffer.MarkAsDirty(insertBlockID)
	return rowID, nil
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

	return newBlockID, nil
}

func (ra *recordAllocator) Remove(rowID core.RowID) error {
	block, err := ra.buffer.FetchBlock(rowID.DataBlockID)
	if err != nil {
		return err
	}

	rb := core.NewRecordBlock(block)
	if err = rb.Remove(rowID.LocalID); err != nil {
		return err
	}

	// TODO: Deal with chained rows

	if rb.TotalRecords() == 0 {
		log.Printf("FREE blockid=%d, prevblockid=%d, nextblockid=%d", block.ID, rb.PrevBlockID(), rb.NextBlockID())
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
