package actions

import (
	log "github.com/Sirupsen/logrus"

	"simplejsondb/core"
	"simplejsondb/dbio"
)

type RecordAllocator interface {
	Run(record *core.Record) error
}

type recordAllocator struct {
	buffer dbio.DataBuffer
}

func NewRecordAllocator(buffer dbio.DataBuffer) RecordAllocator {
	return &recordAllocator{buffer}
}

func (ra *recordAllocator) Run(record *core.Record) error {
	log.Printf("INSERT recordid=%d", record.ID)

	controlDataBlock, err := ra.buffer.FetchBlock(0)
	if err != nil {
		return err
	}

	controlBlock := core.NewControlBlock(controlDataBlock)
	insertBlockID := controlBlock.NextAvailableRecordsDataBlockID()

	// TODO: Check if the record fits the data block fetched. In case it doesn't fit,
	//       "slice" the data into multiple blocks (aka chained rows). Use the amount
	//       of bytes written returned by `recordBlock.Add` to decide
	//       Don't forget to `recordBlock.SetNextRowID(localID, nextBlockID, nextLocalID)`
	//       and `ra.buffer.MarkAsDirty(nextBlockID)`
	//       Also need to take into consideration the linked list of data blocks and
	//       update pointers when allocating a new datablock

	for {
		block, err := ra.buffer.FetchBlock(insertBlockID)
		if err != nil {
			return err
		}
		recordBlock := core.NewRecordBlock(block)
		recordLength := len(record.Data)
		// TODO: Move to the record block object
		freeSpaceForInsert := int(recordBlock.FreeSpace()) - int(core.RECORD_HEADER_SIZE)

		// Does the record fit on the datablock?
		if (freeSpaceForInsert - recordLength) >= 0 {
			recordBlock.Add(record.ID, []byte(record.Data[0:recordLength]))
			// log.Println("New record RowID:", block.ID, localID)
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

		currBlockID := insertBlockID
		blocksMap := core.NewDataBlocksMap(ra.buffer)
		insertBlockID = blocksMap.FirstFree()
		blocksMap.MarkAsUsed(insertBlockID)
		log.Printf("ALLOCATE blockid=%d, prevblockid=%d", insertBlockID, currBlockID)

		// FIXME: Deal with Datafile with no space left

		recordBlock.SetNextBlockID(insertBlockID)
		block, err = ra.buffer.FetchBlock(insertBlockID)
		if err != nil {
			return err
		}
		core.NewRecordBlock(block).SetPrevBlockID(currBlockID)
		ra.buffer.MarkAsDirty(currBlockID)

		controlBlock.SetNextAvailableRecordsDataBlockID(insertBlockID)
		ra.buffer.MarkAsDirty(controlDataBlock.ID)
	}

	ra.buffer.MarkAsDirty(insertBlockID)
	return nil
}
