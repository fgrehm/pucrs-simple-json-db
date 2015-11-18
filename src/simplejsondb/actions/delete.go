package actions

import (
	"simplejsondb/core"
	"simplejsondb/dbio"
)

func Delete(index core.Uint32Index, buffer dbio.DataBuffer, id uint32) error {
	rowID, err := index.Find(id)
	if err != nil {
		return err
	}

	allocator := NewRecordAllocator(buffer)
	if err := allocator.Remove(rowID); err != nil {
		return err
	}

	return index.Delete(id)
}
