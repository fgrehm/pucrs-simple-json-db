package core

type RecordBlockAdapter interface {
	Utilization() uint16
	Add(recordID uint32, data []byte) (uint16, uint16)
}

const RECORD_HEADER_SIZE = uint16(12)

type recordBlockAdapter struct {
	block *DataBlock
}

type recordHeader struct {
	localID  uint16
	recordID uint32
	startsAt uint16
	size     uint16
}

func newRecordBlockAdapter(block *DataBlock) RecordBlockAdapter {
	return &recordBlockAdapter{block}
}

func (rba *recordBlockAdapter) Add(recordID uint32, data []byte) (uint16, uint16) {
	utilization := rba.Utilization()
	recordSize := uint16(len(data))

	// Records present on the block
	totalRecords := rba.block.ReadUint16(DATABLOCK_SIZE - 4)

	// Calculate where the record starts
	var recordPtr int
	if totalRecords == 0 {
		recordPtr = 0
	} else {
		lastHeaderPtr := DATABLOCK_SIZE - 8 - int(totalRecords*RECORD_HEADER_SIZE) - 1
		// Starts where the last record ends
		// FIXME: This will fail once we have deletion implemented
		recordPtr = int(rba.block.ReadUint16(lastHeaderPtr+4) + rba.block.ReadUint16(lastHeaderPtr+6))
	}

	// Header
	// 2 for utilization, 2 for total records, 4 for next / prev block pointers
	newHeaderPtr := DATABLOCK_SIZE - 8
	newHeaderPtr -= int((totalRecords+1)*RECORD_HEADER_SIZE) + 1

	// Le ID
	rba.block.Write(newHeaderPtr, recordID)
	newHeaderPtr += 4

	// Where the record starts
	rba.block.Write(newHeaderPtr, uint16(recordPtr))
	newHeaderPtr += 2

	// Record size
	rba.block.Write(newHeaderPtr, recordSize)
	newHeaderPtr += 2

	// TODO: 4 bytes for chained rows

	// Le data
	rba.block.Write(recordPtr, data)
	totalRecords += 1
	utilization += RECORD_HEADER_SIZE + recordSize
	rba.block.Write(DATABLOCK_SIZE-2, utilization)
	rba.block.Write(DATABLOCK_SIZE-4, totalRecords)

	// Used as the rowid
	localID := totalRecords -1
	bytesWritten := recordSize
	return bytesWritten, localID
}

func (rba *recordBlockAdapter) Utilization() uint16 {
	// A datablock will have at least 2 bytes to store its utilization, if it
	// is currently zero, it means it is a brand new block
	utilization := rba.block.ReadUint16(DATABLOCK_SIZE - 2)
	if utilization == 0 {
		utilization = 2
	}
	return utilization
}
