package core

import (
	log "github.com/Sirupsen/logrus"

	"simplejsondb/dbio"
)

func FormatDataFileIfNeeded(dataFile dbio.DataFile) error {
	dataBuffer := dbio.NewDataBuffer(dataFile, 5)
	repo := NewDataBlockRepository(dataBuffer)

	controlBlock := repo.ControlBlock()
	if controlBlock.NextAvailableRecordsDataBlockID() != 0 {
		log.Println("DB_FORMAT_SKIPPED")
		return nil
	}

	log.Println("DB_FORMAT_DATA_FILE")
	controlBlock.Format()
	dataBuffer.MarkAsDirty(controlBlock.DataBlockID())

	blockMap := repo.DataBlocksMap()
	// 4 -> 1 for the control block
	//      + 2 for the datablocks bitmap
	//      + 1 for the first block used by records
	//      + 1 for the first block used by the btree
	for i := uint16(0); i < 5; i++ {
		blockMap.MarkAsUsed(i)
	}

	return dataBuffer.Sync()
}
