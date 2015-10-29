package core

import (
	"errors"

	"simplejsondb/dbio"
)

type RecordBlock interface {
	FreeSpace() uint16
	Utilization() uint16
	Add(recordID uint32, data []byte) (uint16, uint16)
	// ChainedRowID(localID uint16) RowID
	Remove(localID uint16) error
	NextBlockID() uint16
	SetNextBlockID(blockID uint16)
	PrevBlockID() uint16
	SetPrevBlockID(blockID uint16)
	ReadRecordData(localID uint16) (string, error)

	// HACK: Temporary, meant to be around while we don't have a btree in place
	IDs() []uint32
}

const (
	HEADER_OFFSET_RECORD_ID    = 0
	HEADER_OFFSET_RECORD_START = 4
	HEADER_OFFSET_RECORD_SIZE  = HEADER_OFFSET_RECORD_START + 2
	RECORD_HEADER_SIZE         = uint16(12)

	// A datablock will have at least 8 bytes to store its utilization, total
	// records count and prev / next datablock pointers
	MIN_UTILIZATION = 8

	POS_UTILIZATION   = dbio.DATABLOCK_SIZE - 2
	POS_TOTAL_HEADERS = POS_UTILIZATION - 2
	POS_NEXT_BLOCK    = POS_TOTAL_HEADERS - 2
	POS_PREV_BLOCK    = POS_NEXT_BLOCK - 2
	POS_FIRST_HEADER  = POS_PREV_BLOCK - RECORD_HEADER_SIZE - 1
)

type recordBlock struct {
	block *dbio.DataBlock
}

type recordBlockHeader struct {
	localID  uint16
	recordID uint32
	startsAt uint16
	size     uint16
}

func NewRecordBlock(block *dbio.DataBlock) RecordBlock {
	return &recordBlock{block}
}

func (rb *recordBlock) Add(recordID uint32, data []byte) (uint16, uint16) {
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)

	utilization := rb.Utilization()

	headers := rb.parseHeaders()
	var newHeader *recordBlockHeader

	// Is there a header we can reuse?
	for _, h := range headers {
		if h.recordID == 0 {
			newHeader = &h
			break
		}
	}
	if newHeader == nil {
		if totalHeaders == 0 {
			newHeader = &recordBlockHeader{
				localID:  0,
				recordID: recordID,
				// FIXME: This is wrong
				startsAt: 0,
				// FIXME: This is wrong
				size:     uint16(len(data)),
			}
		} else {
			// FIXME: This is wrong
			lastHeaderPtr := int(POS_FIRST_HEADER) - int((totalHeaders-1)*RECORD_HEADER_SIZE)
			newHeader = &recordBlockHeader{
				localID:  totalHeaders,
				recordID: recordID,
				// FIXME: This is wrong
				startsAt: rb.block.ReadUint16(lastHeaderPtr+HEADER_OFFSET_RECORD_START) + rb.block.ReadUint16(lastHeaderPtr+HEADER_OFFSET_RECORD_SIZE),
				// FIXME: This is wrong
				size:     uint16(len(data)),
			}
		}
		totalHeaders += 1
		utilization += RECORD_HEADER_SIZE
	} else {
		// FIXME: This is wrong
		lastHeaderPtr := int(POS_FIRST_HEADER) - int((totalHeaders-1)*RECORD_HEADER_SIZE)

		newHeader.recordID = recordID

		// FIXME: This is wrong
		newHeader.startsAt = rb.block.ReadUint16(lastHeaderPtr+HEADER_OFFSET_RECORD_START) + rb.block.ReadUint16(lastHeaderPtr+HEADER_OFFSET_RECORD_SIZE)
		// FIXME: This is wrong
		newHeader.size = uint16(len(data))
	}

	newHeaderPtr := int(POS_FIRST_HEADER) - int(newHeader.localID*RECORD_HEADER_SIZE)

	// Le ID
	rb.block.Write(newHeaderPtr+HEADER_OFFSET_RECORD_ID, newHeader.recordID)

	// Where the record starts
	rb.block.Write(newHeaderPtr+HEADER_OFFSET_RECORD_START, newHeader.startsAt)

	// Record size
	rb.block.Write(newHeaderPtr+HEADER_OFFSET_RECORD_SIZE, newHeader.size)

	// TODO: 4 bytes for chained rows

	// Le data
	rb.block.Write(int(newHeader.startsAt), data)
	utilization += newHeader.size
	rb.block.Write(POS_UTILIZATION, utilization)
	rb.block.Write(POS_TOTAL_HEADERS, totalHeaders)

	bytesWritten := newHeader.size
	return bytesWritten, newHeader.localID
}

func (rb *recordBlock) Remove(localID uint16) error {
	// Records present on the block
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	if localID >= totalHeaders {
		return errors.New("Invalid local ID provided to `RecordBlock.Remove`")
	}

	headerPtr := int(POS_FIRST_HEADER) - int(localID*RECORD_HEADER_SIZE)
	rb.block.Write(headerPtr+HEADER_OFFSET_RECORD_ID, uint32(0))

	// Utilization goes down just by the amount of data taken by the record, the
	// header is kept around so we do not "free" up the space taken by it
	utilization := rb.Utilization() - rb.block.ReadUint16(headerPtr+HEADER_OFFSET_RECORD_SIZE)
	rb.block.Write(POS_UTILIZATION, utilization)

	return nil
}

func (rb *recordBlock) Utilization() uint16 {
	utilization := rb.block.ReadUint16(POS_UTILIZATION)
	if utilization == 0 {
		utilization = MIN_UTILIZATION
	}
	return utilization
}

func (rb *recordBlock) NextBlockID() uint16 {
	return rb.block.ReadUint16(POS_NEXT_BLOCK)
}

func (rb *recordBlock) SetNextBlockID(blockID uint16) {
	rb.block.Write(POS_NEXT_BLOCK, blockID)
}

func (rb *recordBlock) PrevBlockID() uint16 {
	return rb.block.ReadUint16(POS_PREV_BLOCK)
}

func (rb *recordBlock) SetPrevBlockID(blockID uint16) {
	rb.block.Write(POS_PREV_BLOCK, blockID)
}

func (rb *recordBlock) ReadRecordData(localID uint16) (string, error) {
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	if localID >= totalHeaders {
		return "", errors.New("Invalid local ID provided to `RecordBlock.ReadRecordData`")
	}

	headerPtr := int(POS_FIRST_HEADER) - int(localID*RECORD_HEADER_SIZE)
	id := rb.block.ReadUint32(headerPtr + HEADER_OFFSET_RECORD_ID)
	if id == 0 {
		return "", errors.New("Invalid local ID provided to `RecordBlock.ReadRecordData`")
	}

	start := rb.block.ReadUint16(headerPtr + HEADER_OFFSET_RECORD_START)
	end := start + rb.block.ReadUint16(headerPtr+HEADER_OFFSET_RECORD_SIZE)
	return string(rb.block.Data[start:end]), nil
}

func (rb *recordBlock) FreeSpace() uint16 {
	return dbio.DATABLOCK_SIZE - rb.Utilization()
}

func (rb *recordBlock) parseHeaders() []recordBlockHeader {
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	ret := []recordBlockHeader{}

	for localID := uint16(0); localID < totalHeaders; localID++ {
		headerPtr := int(POS_FIRST_HEADER - localID*RECORD_HEADER_SIZE)
		header := recordBlockHeader{
			localID:  localID,
			recordID: rb.block.ReadUint32(headerPtr + HEADER_OFFSET_RECORD_ID),
			startsAt: rb.block.ReadUint16(headerPtr + HEADER_OFFSET_RECORD_START),
			size:     rb.block.ReadUint16(headerPtr + HEADER_OFFSET_RECORD_SIZE),
		}
		ret = append(ret, header)
	}

	return ret
}

// HACK: Temporary, meant to be around while we don't have a btree in place
func (rb *recordBlock) IDs() []uint32 {
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	ids := []uint32{}

	for i := uint16(0); i < totalHeaders; i++ {
		headerPtr := int(POS_FIRST_HEADER - i*RECORD_HEADER_SIZE)
		id := rb.block.ReadUint32(headerPtr + HEADER_OFFSET_RECORD_ID)
		ids = append(ids, id)
	}

	return ids
}
