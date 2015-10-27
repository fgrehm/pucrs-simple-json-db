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
	block, err := ra.buffer.FetchBlock(0)
	if err != nil {
		return err
	}

	cb := core.NewControlBlock(block)
	insertBlockID := cb.NextAvailableRecordsDataBlockID()

	// TODO: Check if the record fits the data block fetched. In case it doesn't fit,
	//       "slice" the data into multiple blocks (aka chained rows). Use the amount
	//       of bytes written returned by `adapter.Add` to decide
	//       Don't forget to `adapter.SetNextRowID(localID, nextBlockID, nextLocalID)`
	//       and `ra.buffer.MarkAsDirty(nextBlockID)`
	//       Also need to take into consideration the linked list of data blocks and
	//       update pointers when allocating a new datablock

	for {
		block, err := ra.buffer.FetchBlock(insertBlockID)
		if err != nil {
			return err
		}
		adapter := core.NewRecordBlockAdapter(block)

		fitsOnDataBlock := (int(adapter.FreeSpace()) - len(record.Data) - int(core.RECORD_HEADER_SIZE)) > 0
		if fitsOnDataBlock {
			_, _ = adapter.Add(record.ID, []byte(record.Data[0:len(record.Data)]))
			// log.Println("New record RowID:", block.ID, localID)
			break
		}

		if nextID := adapter.NextBlockID(); nextID != 0 {
			insertBlockID = nextID
			continue
		}

		// FIXME: Deal with Datafile with no space left

		currBlockID := insertBlockID
		insertBlockID++
		log.Printf("Allocating a new datablock (%d)", insertBlockID)
		adapter.SetNextBlockID(insertBlockID)
		block, err = ra.buffer.FetchBlock(insertBlockID)
		if err != nil {
			return err
		}
		core.NewRecordBlockAdapter(block).SetPrevBlockID(currBlockID)

		ra.buffer.MarkAsDirty(currBlockID)
	}

	ra.buffer.MarkAsDirty(insertBlockID)
	return nil
}
