package core

type RecordAllocator interface {
	Run(record *Record) error
}

type recordAllocator struct {
	buffer DataBuffer
}

func newRecordAllocator(buffer DataBuffer) RecordAllocator {
	return &recordAllocator{buffer}
}

func (ra *recordAllocator) Run(record *Record) error {
	block, err := ra.buffer.FetchBlock(0)
	if err != nil {
		return err
	}
	insertBlockId := block.ReadUint16(4)

	initialDataBlock, err := ra.buffer.FetchBlock(insertBlockId)
	if err != nil {
		return err
	}

	utilization := ra.readUtilization(initialDataBlock)
	recordSize := uint16(len(record.Data))

	// Records present on the block
	totalRecords := initialDataBlock.ReadUint16(DATABLOCK_SIZE-4)

	// Calculate where the record starts
	var recordPtr int
	if totalRecords == 0 {
		recordPtr = 0
	} else {
		lastHeaderPtr := DATABLOCK_SIZE - 8 - int(totalRecords*RECORD_HEADER_SIZE) - 1
		// Starts where the last record ends
		// FIXME: This will fail once we have deletion implemented
		recordPtr = int(initialDataBlock.ReadUint16(lastHeaderPtr+4) + initialDataBlock.ReadUint16(lastHeaderPtr+6))
	}

	// Header
	// 2 for utilization, 2 for total records, 4 for next / prev block pointers
	newHeaderPtr := DATABLOCK_SIZE - 8
	newHeaderPtr -= int((totalRecords+1)*RECORD_HEADER_SIZE) + 1

	// Le ID
	initialDataBlock.Write(newHeaderPtr, record.ID)
	newHeaderPtr += 4

	// Where the record starts
	initialDataBlock.Write(newHeaderPtr, uint16(recordPtr))
	newHeaderPtr += 2

	// Record size
	initialDataBlock.Write(newHeaderPtr, recordSize)
	newHeaderPtr += 2

	// TODO: 4 bytes for chained rows

	// Le data
	initialDataBlock.Write(recordPtr, record.Data)
	totalRecords += 1
	utilization += RECORD_HEADER_SIZE + recordSize
	initialDataBlock.Write(DATABLOCK_SIZE-2, utilization)
	initialDataBlock.Write(DATABLOCK_SIZE-4, totalRecords)
	ra.buffer.MarkAsDirty(initialDataBlock.ID)

	// - Records data
	// - End the end of the datablock:
	//   - 4 bytes for pointer to previous and next data blocks on the linked list of data blocks of a given type (index or actual data, 2 points each)

	return nil
}

func (ra *recordAllocator) readUtilization(block *DataBlock) uint16 {
	// A datablock will have at least 2 bytes to store its utilization, if it
	// is currently zero, it means it is a brand new block
	utilization := block.ReadUint16(DATABLOCK_SIZE - 2)
	if utilization == 0 {
		utilization = 2
	}
	return utilization
}
