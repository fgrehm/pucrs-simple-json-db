package actions

import (
	"simplejsondb/core"
	"simplejsondb/dbio"
)

func Update(index core.Uint32Index, buffer dbio.DataBuffer, record *core.Record) error {
	rowID, err := index.Find(record.ID)
	if err != nil {
		return err
	}

	allocator := NewRecordAllocator(buffer)
	if err = allocator.Update(rowID, record); err != nil {
		return err
	}

	return nil
}
