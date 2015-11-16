package actions

import (
  "errors"

  "simplejsondb/core"
  "simplejsondb/dbio"

  log "github.com/Sirupsen/logrus"
)

func Find(buffer dbio.DataBuffer, id uint32) (*core.Record, error) {
  rowID, err := findRowID(buffer, id)
  if err != nil {
    return nil, err
  }

  return core.NewRecordFinder(buffer).Find(id, rowID)
}

// HACK: Temporary workaround while we don't have the BTree+ in place
func findRowID(buffer dbio.DataBuffer, needle uint32) (core.RowID, error) {
  log.Debugf("Looking up the RowID for %d", needle)
  repo := core.NewDataBlockRepository(buffer)

  blockID := repo.ControlBlock().FirstRecordDataBlock()
  for {
    rb := repo.RecordBlock(blockID)
    for i, id := range rb.IDs() {
      if id == needle {
        return core.RowID{DataBlockID: blockID, LocalID: uint16(i)}, nil
      }
    }

    blockID = rb.NextBlockID()
    log.Debugf("Reading the next block %d", blockID)
    if blockID == 0 {
      return core.RowID{}, errors.New("Not found")
    }
  }
}
