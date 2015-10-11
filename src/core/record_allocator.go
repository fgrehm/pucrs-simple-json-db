package core

import (
	"log"
)

type RecordAllocator interface {
	Run(record *Record) error
}

type recordAllocator struct {
	buffer DataBuffer
}

func newRecordAllocator(buffer DataBuffer) RecordAllocator {
	return &recordAllocator{buffer}
}

func (ra *recordAllocator) Run(record *Record) error {
	block, err := ra.buffer.FetchBlock(0)
	if err != nil {
		return err
	}
	insertBlockId := block.ReadUint16(4)

	initialDataBlock, err := ra.buffer.FetchBlock(insertBlockId)
	if err != nil {
		return err
	}

	// TODO: Check if the record fits the data block fetched. In case it doesn't fit,
	//       "slice" the data into multiple blocks (aka chained rows). Use the amount
	//       of bytes written returned by `adapter.Add` to decide
	//       Don't forget to `adapter.SetNextRowID(localID, nextBlockID, nextLocalID)`
	//       and `ra.buffer.MarkAsDirty(nextBlockID)`

	adapter := newRecordBlockAdapter(initialDataBlock)
	_, localID := adapter.Add(record.ID, []byte(record.Data[0:len(record.Data)]))
	log.Println("New record RowID:", initialDataBlock.ID, localID)

	ra.buffer.MarkAsDirty(initialDataBlock.ID)

	return nil
}
