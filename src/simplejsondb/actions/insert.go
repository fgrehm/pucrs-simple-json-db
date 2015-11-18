package actions

import (
	"fmt"
	"simplejsondb/core"
	"simplejsondb/dbio"
)

func Insert(index core.Uint32Index, buffer dbio.DataBuffer, record *core.Record) error {
	cb := core.NewDataBlockRepository(buffer).ControlBlock()
	buffer.MarkAsDirty(cb.DataBlockID())

	rowID, _ := index.Find(record.ID)
	if rowID != (core.RowID{}) {
		return fmt.Errorf("Key already exists: %d", record.ID)
	}

	allocator := core.NewRecordAllocator(buffer)
	rowID, err := allocator.Add(record)
	if err != nil {
		return err
	}

	return index.Insert(record.ID, rowID)
}
