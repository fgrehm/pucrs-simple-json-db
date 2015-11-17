package actions

import (
	"simplejsondb/core"
	"simplejsondb/dbio"
)

func Insert(buffer dbio.DataBuffer, record *core.Record) error {
	cb := core.NewDataBlockRepository(buffer).ControlBlock()
	buffer.MarkAsDirty(cb.DataBlockID())

	allocator := NewRecordAllocator(buffer)
	if _, err := allocator.Add(record); err != nil {
		return err
	}

	// TODO: After inserting the record, need to update the BTree+ index

	return nil
}
