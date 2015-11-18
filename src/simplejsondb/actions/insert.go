package actions

import (
	"simplejsondb/core"
	"simplejsondb/dbio"
)

func Insert(index core.Uint32Index, buffer dbio.DataBuffer, record *core.Record) error {
	cb := core.NewDataBlockRepository(buffer).ControlBlock()
	buffer.MarkAsDirty(cb.DataBlockID())

	allocator := NewRecordAllocator(buffer)
	rowID, err := allocator.Add(record)
	if err != nil {
		return err
	}

	return index.Insert(record.ID, rowID)
}
