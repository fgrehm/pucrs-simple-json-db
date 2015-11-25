package actions

import (
  "simplejsondb/core"
  "simplejsondb/dbio"
)

func Search(index core.Uint32Index, buffer dbio.DataBuffer, key, value string) ([]*core.Record, error) {
  results := []*core.Record{}
  index.All(func (id uint32, rowID core.RowID) {
    record, err := core.NewRecordLoader(buffer).Load(id, rowID)
    if err != nil {
      // Ideally should recover but that means lots of stuff needs to be changed
      panic(err)
    }

    json, err := record.ParseJSON()
    if err != nil {
      panic(err)
    }

    jsonValue, ok := json[key]
    if !ok {
      return
    }
    if jsonValue == value {
      results = append(results, record)
    }
  })

  return results, nil
}
