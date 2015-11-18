package core

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"sort"

	"simplejsondb/dbio"
)

type RecordBlock interface {
	DataBlockID() uint16
	FreeSpaceForInsert() uint16
	Utilization() uint16
	TotalRecords() int
	Add(recordID uint32, data []byte) uint16
	SetChainedRowID(localID uint16, rowID RowID) error
	ChainedRowID(localID uint16) (RowID, error)
	Remove(localID uint16) error
	SoftRemove(localID uint16) error
	NextBlockID() uint16
	SetNextBlockID(blockID uint16)
	PrevBlockID() uint16
	SetPrevBlockID(blockID uint16)
	ReadRecordData(localID uint16) ([]byte, error)
	Clear()

	// HACK: Temporary, meant to be around while we don't have a btree in place
	IDs() []uint32
}

const (
	HEADER_OFFSET_RECORD_ID            = 0
	HEADER_OFFSET_RECORD_START         = 4
	HEADER_OFFSET_RECORD_SIZE          = HEADER_OFFSET_RECORD_START + 2
	HEADER_OFFSET_CHAINED_ROW_BLOCK_ID = HEADER_OFFSET_RECORD_SIZE + 2
	HEADER_OFFSET_CHAINED_ROW_LOCAL_ID = HEADER_OFFSET_CHAINED_ROW_BLOCK_ID + 2
	RECORD_HEADER_SIZE                 = uint16(12)

	// A datablock will have at least 8 bytes to store its utilization, total
	// records count and prev / next datablock pointers
	MIN_UTILIZATION = 8

	POS_UTILIZATION   = dbio.DATABLOCK_SIZE - 2
	POS_TOTAL_HEADERS = POS_UTILIZATION - 2
	POS_NEXT_BLOCK    = POS_TOTAL_HEADERS - 2
	POS_PREV_BLOCK    = POS_NEXT_BLOCK - 2
	POS_FIRST_HEADER  = POS_PREV_BLOCK - RECORD_HEADER_SIZE
)

type recordBlock struct {
	block *dbio.DataBlock
}

type recordBlockHeader struct {
	localID        uint16
	recordID       uint32
	startsAt       uint16
	size           uint16
	chainedBlockID uint16
	chainedLocalID uint16
}

type recordBlockHeaders []*recordBlockHeader

func (rb *recordBlock) DataBlockID() uint16 {
	return rb.block.ID
}

// REFACTOR: This method is doing way too much
func (rb *recordBlock) Add(recordID uint32, data []byte) uint16 {
	headers := rb.parseHeaders()
	utilization := rb.Utilization()

	var newHeader *recordBlockHeader
	sort.Sort(headers)

	// Do we have the record on the block
	for _, h := range headers {
		if h.recordID == recordID {
			newHeader = h
			break
		}
	}
	// Or maybe we have a gap we can reuse?
	if newHeader == nil {
		for _, h := range headers {
			if h.recordID == 0 {
				newHeader = h
				break
			}
		}
	}

	if newHeader == nil {
		newHeader = &recordBlockHeader{
			localID:  uint16(len(headers)),
			recordID: recordID,
			size:     uint16(len(data)),
		}
		headers = append(headers, newHeader)
		utilization += RECORD_HEADER_SIZE
	} else {
		rb.defragment(headers)
		newHeader.size = uint16(len(data))
		newHeader.recordID = recordID
	}
	newHeader.startsAt = utilization - MIN_UTILIZATION - (uint16(len(headers)) * RECORD_HEADER_SIZE)
	log.Infof("WRITE rowid='%d:%d', recordid=%d, startsAt=%d, size=%d",
		rb.block.ID,
		newHeader.localID,
		newHeader.recordID,
		newHeader.startsAt,
		newHeader.size)

	newHeaderPtr := int(POS_FIRST_HEADER) - int(newHeader.localID*RECORD_HEADER_SIZE)

	// Le ID
	rb.block.Write(newHeaderPtr+HEADER_OFFSET_RECORD_ID, newHeader.recordID)

	// Where the record starts
	rb.block.Write(newHeaderPtr+HEADER_OFFSET_RECORD_START, newHeader.startsAt)

	// Record size
	rb.block.Write(newHeaderPtr+HEADER_OFFSET_RECORD_SIZE, newHeader.size)

	// Always zero out chained row IDs, in case we are reusing a deleted record header
	rb.block.Write(newHeaderPtr+HEADER_OFFSET_CHAINED_ROW_BLOCK_ID, uint16(0))
	rb.block.Write(newHeaderPtr+HEADER_OFFSET_CHAINED_ROW_LOCAL_ID, uint16(0))

	// Le data
	rb.block.Write(int(newHeader.startsAt), data)
	utilization += newHeader.size

	totalHeaders := uint16(len(headers))

	log.Infof("WRITE blockid=%d, utilization='%dbytes', totalheaders=%d", rb.block.ID, utilization, totalHeaders)

	rb.block.Write(POS_UTILIZATION, utilization)
	rb.block.Write(POS_TOTAL_HEADERS, totalHeaders)

	// log.Debugf("HEADERS %s", rb.parseHeaders())

	return newHeader.localID
}

func (rb *recordBlock) Remove(localID uint16) error {
	// Records present on the block
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	if localID >= totalHeaders {
		return errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.Remove`, got %d, totalheaders is %d", localID, totalHeaders))
	}

	headerPtr := int(POS_FIRST_HEADER) - int(localID*RECORD_HEADER_SIZE)
	log.Infof("DELETE rowid='%d:%d', recordid=%d, startsAt=%d, size=%d",
		rb.block.ID,
		localID,
		rb.block.ReadUint32(headerPtr+HEADER_OFFSET_RECORD_ID),
		rb.block.ReadUint16(headerPtr+HEADER_OFFSET_RECORD_START),
		rb.block.ReadUint16(headerPtr+HEADER_OFFSET_RECORD_SIZE))

	rb.block.Write(headerPtr+HEADER_OFFSET_RECORD_ID, uint32(0))

	// Utilization goes down just by the amount of data taken by the record, the
	// header is kept around so we do not "free" up the space taken by it
	utilization := rb.Utilization() - rb.block.ReadUint16(headerPtr+HEADER_OFFSET_RECORD_SIZE)
	rb.block.Write(POS_UTILIZATION, utilization)

	log.Infof("WRITE blockid=%d, utilization='%dbytes', totalheaders=%d", rb.block.ID, utilization, totalHeaders)

	return nil
}

func (rb *recordBlock) SoftRemove(localID uint16) error {
	// Records present on the block
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	if localID >= totalHeaders {
		return errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.SoftRemove`, got %d, totalheaders is %d", localID, totalHeaders))
	}

	headerPtr := int(POS_FIRST_HEADER) - int(localID)*int(RECORD_HEADER_SIZE)
	log.Infof("SOFT_DELETE rowid='%d:%d', recordid=%d, startsAt=%d, size=%d",
		rb.block.ID,
		localID,
		rb.block.ReadUint32(headerPtr+HEADER_OFFSET_RECORD_ID),
		rb.block.ReadUint16(headerPtr+HEADER_OFFSET_RECORD_START),
		rb.block.ReadUint16(headerPtr+HEADER_OFFSET_RECORD_SIZE))

	currrentRecordSize := rb.block.ReadUint16(headerPtr + HEADER_OFFSET_RECORD_SIZE)
	rb.block.Write(headerPtr+HEADER_OFFSET_RECORD_SIZE, uint32(0))
	rb.block.Write(headerPtr+HEADER_OFFSET_CHAINED_ROW_BLOCK_ID, uint32(0))
	rb.block.Write(headerPtr+HEADER_OFFSET_CHAINED_ROW_LOCAL_ID, uint32(0))

	// Utilization goes down just by the amount of data taken by the record, the
	// header is kept around so we do not "free" up the space taken by it
	utilization := rb.Utilization() - currrentRecordSize
	rb.block.Write(POS_UTILIZATION, utilization)

	log.Infof("SOFT_DELETE_WRITE blockid=%d, utilization='%dbytes', totalheaders=%d", rb.block.ID, utilization, totalHeaders)

	return nil
}

func (rb *recordBlock) Clear() {
	rb.block.Write(POS_TOTAL_HEADERS, uint16(0))
	rb.block.Write(POS_UTILIZATION, uint16(0))
}

func (rb *recordBlock) Utilization() uint16 {
	utilization := rb.block.ReadUint16(POS_UTILIZATION)
	if utilization == 0 {
		utilization = MIN_UTILIZATION
	}
	return utilization
}

func (rb *recordBlock) TotalRecords() int {
	records := 0
	for _, h := range rb.parseHeaders() {
		if h.recordID != 0 {
			records += 1
		}
	}
	return records
}

func (rb *recordBlock) NextBlockID() uint16 {
	return rb.block.ReadUint16(POS_NEXT_BLOCK)
}

func (rb *recordBlock) SetNextBlockID(blockID uint16) {
	log.Debugf("Setting %d next block id to %d", rb.block.ID, blockID)
	rb.block.Write(POS_NEXT_BLOCK, blockID)
}

func (rb *recordBlock) PrevBlockID() uint16 {
	return rb.block.ReadUint16(POS_PREV_BLOCK)
}

func (rb *recordBlock) SetPrevBlockID(blockID uint16) {
	log.Debugf("Setting %d prev block id to %d", rb.block.ID, blockID)
	rb.block.Write(POS_PREV_BLOCK, blockID)
}

func (rb *recordBlock) ChainedRowID(localID uint16) (RowID, error) {
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	if localID >= totalHeaders {
		return RowID{}, errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.ChainedRowID` (%d)", localID))
	}

	headerPtr := int(POS_FIRST_HEADER) - int(localID)*int(RECORD_HEADER_SIZE)
	id := rb.block.ReadUint32(headerPtr + HEADER_OFFSET_RECORD_ID)
	if id == 0 {
		return RowID{}, errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.ChainedRowID` (%d)", localID))
	}

	return RowID{
		DataBlockID: rb.block.ReadUint16(headerPtr + HEADER_OFFSET_CHAINED_ROW_BLOCK_ID),
		LocalID:     rb.block.ReadUint16(headerPtr + HEADER_OFFSET_CHAINED_ROW_LOCAL_ID),
	}, nil
}

func (rb *recordBlock) SetChainedRowID(localID uint16, rowID RowID) error {
	log.Printf("SET_CHAINED from='%d:%d', to='%d:%d'", rb.block.ID, localID, rowID.DataBlockID, rowID.LocalID)
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	if localID >= totalHeaders {
		return errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.SetChainedRowID` (%d)", localID))
	}

	headerPtr := int(POS_FIRST_HEADER) - int(localID)*int(RECORD_HEADER_SIZE)
	id := rb.block.ReadUint32(headerPtr + HEADER_OFFSET_RECORD_ID)
	if id == 0 {
		return errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.SetChainedRowID` (%d)", localID))
	}

	rb.block.Write(headerPtr+HEADER_OFFSET_CHAINED_ROW_BLOCK_ID, rowID.DataBlockID)
	rb.block.Write(headerPtr+HEADER_OFFSET_CHAINED_ROW_LOCAL_ID, rowID.LocalID)

	return nil
}

func (rb *recordBlock) ReadRecordData(localID uint16) ([]byte, error) {
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	if localID >= totalHeaders {
		return nil, errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.ReadRecordData` (%d)", localID))
	}

	headerPtr := int(POS_FIRST_HEADER) - int(localID)*int(RECORD_HEADER_SIZE)
	id := rb.block.ReadUint32(headerPtr + HEADER_OFFSET_RECORD_ID)
	if id == 0 {
		return nil, errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.ReadRecordData` (%d)", localID))
	}

	start := rb.block.ReadUint16(headerPtr + HEADER_OFFSET_RECORD_START)
	recordSize := rb.block.ReadUint16(headerPtr + HEADER_OFFSET_RECORD_SIZE)
	end := start + recordSize

	log.Infof("READ rowid='%d:%d', recordid=%d, startsAt=%d, size=%d", rb.block.ID, localID, id, start, recordSize)

	return rb.block.Data[start:end], nil
}

func (rb *recordBlock) FreeSpaceForInsert() uint16 {
	freeSpace := dbio.DATABLOCK_SIZE - rb.Utilization()

	// Do we need a new header?
	newHeaderNeeded := true
	for _, h := range rb.parseHeaders() {
		if h.recordID == 0 {
			newHeaderNeeded = false
			break
		}
	}

	if newHeaderNeeded {
		if freeSpace > RECORD_HEADER_SIZE {
			freeSpace -= RECORD_HEADER_SIZE
		} else {
			freeSpace = 0
		}
	}

	return freeSpace
}

func (rb *recordBlock) defragment(headers recordBlockHeaders) {
	log.Infof("DEFRAG blockid=%d", rb.block.ID)

	sort.Sort(headers)
	// log.Debugf("Block headers before defrag: %s", headers)
	for i, h := range headers {
		// Skip headers that repesent data
		if h.recordID != 0 && h.size != 0 {
			continue
		}

		// If the header has been zeroed out already, move on
		if h.recordID == 0 && h.size == 0 {
			continue
		}

		// Shift all of the following headers data
		dataPtr := h.startsAt
		for _, n := range headers[i+1:] {
			log.Debugf("COPY blockid=%d, from=%d, to=%d, size=%d", rb.block.ID, n.startsAt, dataPtr, n.size)
			// Copy bytes over from the following record
			for p := uint16(0); p < n.size; p++ {
				rb.block.Data[dataPtr+p] = rb.block.Data[n.startsAt+p]
			}
			n.startsAt = dataPtr
			rb.block.Write(int(POS_FIRST_HEADER)-int(n.localID)*int(RECORD_HEADER_SIZE)+int(HEADER_OFFSET_RECORD_START), n.startsAt)
			dataPtr += n.size
		}

		// After compressing, we flag the header as zeroed out
		h.size = 0
		rb.block.Write(int(POS_FIRST_HEADER)-int(h.localID)*int(RECORD_HEADER_SIZE)+int(HEADER_OFFSET_RECORD_SIZE), h.size)
		h.startsAt = dataPtr
		rb.block.Write(int(POS_FIRST_HEADER)-int(h.localID)*int(RECORD_HEADER_SIZE)+int(HEADER_OFFSET_RECORD_START), h.startsAt)
	}
	// log.Debugf("Block headers after defrag: %s", rb.parseHeaders())
	log.Infof("END_DEFRAG blockid=%d", rb.block.ID)
}

func (rbh recordBlockHeaders) Len() int {
	return len(rbh)
}

func (rbh recordBlockHeaders) Less(i, j int) bool {
	return rbh[i].startsAt < rbh[j].startsAt
}

func (rbh recordBlockHeaders) Swap(i, j int) {
	rbh[i], rbh[j] = rbh[j], rbh[i]
}

func (rb *recordBlock) parseHeaders() recordBlockHeaders {
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	ret := recordBlockHeaders{}

	for localID := uint16(0); localID < totalHeaders; localID++ {
		headerPtr := int(POS_FIRST_HEADER - localID*RECORD_HEADER_SIZE)
		header := &recordBlockHeader{
			localID:        localID,
			recordID:       rb.block.ReadUint32(headerPtr + HEADER_OFFSET_RECORD_ID),
			startsAt:       rb.block.ReadUint16(headerPtr + HEADER_OFFSET_RECORD_START),
			size:           rb.block.ReadUint16(headerPtr + HEADER_OFFSET_RECORD_SIZE),
			chainedBlockID: rb.block.ReadUint16(headerPtr + HEADER_OFFSET_CHAINED_ROW_BLOCK_ID),
			chainedLocalID: rb.block.ReadUint16(headerPtr + HEADER_OFFSET_CHAINED_ROW_LOCAL_ID),
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
		headerPtr := int(POS_FIRST_HEADER) - int(i)*int(RECORD_HEADER_SIZE)
		id := rb.block.ReadUint32(headerPtr + HEADER_OFFSET_RECORD_ID)
		ids = append(ids, id)
	}

	return ids
}
