package actions

import (
	"simplejsondb/dbio"
)

func Delete(buffer dbio.DataBuffer, id uint32) error {
	rowID, err := findRowID(buffer, id)
	if err != nil {
		return err
	}

	allocator := NewRecordAllocator(buffer)
	return allocator.Remove(rowID)
}
