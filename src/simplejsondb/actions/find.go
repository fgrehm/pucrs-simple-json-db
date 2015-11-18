package actions

import (
	"simplejsondb/core"
	"simplejsondb/dbio"
)

func Find(index core.Uint32Index, buffer dbio.DataBuffer, id uint32) (*core.Record, error) {
	rowID, err := index.Find(id)
	if err != nil {
		return nil, err
	}

	return core.NewRecordLoader(buffer).Load(id, rowID)
}
