package core

import (
	"errors"
	"fmt"
	log "github.com/Sirupsen/logrus"
	"sort"

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

type recordBlockHeaders []*recordBlockHeader

func NewRecordBlock(block *dbio.DataBlock) RecordBlock {
	return &recordBlock{block}
}

func (rb *recordBlock) Add(recordID uint32, data []byte) (uint16, uint16) {
	headers := rb.parseHeaders()

	var newHeader *recordBlockHeader
	log.Printf("[%d]    add start headers: %s", recordID, headers)

	utilization := rb.Utilization()
	log.Printf("utilization", utilization)

	// Is there a header we can reuse?
	sort.Sort(headers)
	log.Printf("add sorted headers: %s", headers)
	for _, h := range headers {
		if h.recordID == 0 {
			newHeader = h
			break
		}
	}

	if newHeader == nil {
		newHeader = &recordBlockHeader{
			localID:  uint16(len(headers)),
			recordID: recordID,
			size:     uint16(len(data)),
		}
		log.Println("New record header: ", newHeader)
		headers = append(headers, newHeader)
		utilization += RECORD_HEADER_SIZE
	} else {
		rb.defragment(headers)
		sort.Sort(headers)
		newHeader.size = uint16(len(data))
		newHeader.recordID = recordID
	}
	newHeader.startsAt = utilization - MIN_UTILIZATION - (uint16(len(headers)) * RECORD_HEADER_SIZE)
	log.Printf("add end headers: %s", headers)

	// fmt.Printf("%+v\n", headers)
	// fmt.Printf("will insert on %+v\n", newHeader)

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
	log.Printf("end utilization", utilization)
	rb.block.Write(POS_UTILIZATION, utilization)
	rb.block.Write(POS_TOTAL_HEADERS, uint16(len(headers)))

	bytesWritten := newHeader.size
	return bytesWritten, newHeader.localID
}

func (rb *recordBlock) Remove(localID uint16) error {
	log.Printf("begin remove headers: %s", rb.parseHeaders())

	log.Printf("begin utilization", rb.Utilization())

	// Records present on the block
	totalHeaders := rb.block.ReadUint16(POS_TOTAL_HEADERS)
	if localID >= totalHeaders {
		return errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.Remove`, got %d", localID))
	}

	headerPtr := int(POS_FIRST_HEADER) - int(localID*RECORD_HEADER_SIZE)
	rb.block.Write(headerPtr+HEADER_OFFSET_RECORD_ID, uint32(0))

	// Utilization goes down just by the amount of data taken by the record, the
	// header is kept around so we do not "free" up the space taken by it
	utilization := rb.Utilization() - rb.block.ReadUint16(headerPtr+HEADER_OFFSET_RECORD_SIZE)
	rb.block.Write(POS_UTILIZATION, utilization)

	log.Printf("end remove headers: %s", rb.parseHeaders())
	log.Printf("end utilization", utilization)

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
		return "", errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.ReadRecordData` (%d)", localID))
	}

	headerPtr := int(POS_FIRST_HEADER) - int(localID)*int(RECORD_HEADER_SIZE)
	id := rb.block.ReadUint32(headerPtr + HEADER_OFFSET_RECORD_ID)
	if id == 0 {
		return "", errors.New(fmt.Sprintf("Invalid local ID provided to `RecordBlock.ReadRecordData` (%d)", localID))
	}

	start := rb.block.ReadUint16(headerPtr + HEADER_OFFSET_RECORD_START)
	end := start + rb.block.ReadUint16(headerPtr+HEADER_OFFSET_RECORD_SIZE)
	return string(rb.block.Data[start:end]), nil
}

func (rb *recordBlock) FreeSpace() uint16 {
	return dbio.DATABLOCK_SIZE - rb.Utilization()
}

func (rb *recordBlock) defragment(headers recordBlockHeaders) {
	log.Infof("Defragmenting block %d", rb.block.ID)
	sort.Sort(headers)
	log.Infof("Defragmenting block sorted %s", headers)

	for i, h := range headers {
		// Search for a blanky header
		if h.recordID != 0 {
			continue
		}

		// If the header has been zeroed out already, move on
		if h.size == 0 {
			continue
		}

		// Shift all of the following headers data
		log.Debugf("Compressing byte range: %d-%d", h.startsAt, h.startsAt+h.size)
		dataPtr := h.startsAt
		for _, n := range headers[i+1:] {
			// Copy bytes over from the following record
			for p := uint16(0); p < n.size; p++ {
				rb.block.Data[dataPtr+p] = rb.block.Data[n.startsAt+p]
			}
			n.startsAt = dataPtr
			rb.block.Write(int(POS_FIRST_HEADER)-int(n.localID)*int(RECORD_HEADER_SIZE)+HEADER_OFFSET_RECORD_START, n.startsAt)
			dataPtr += n.size
		}

		// After compressing, we flag the header as zeroed out
		h.size = 0
		rb.block.Write(int(POS_FIRST_HEADER)-int(h.localID)*int(RECORD_HEADER_SIZE)+HEADER_OFFSET_RECORD_SIZE, h.size)
		h.startsAt = dataPtr
		rb.block.Write(int(POS_FIRST_HEADER)-int(h.localID)*int(RECORD_HEADER_SIZE)+HEADER_OFFSET_RECORD_START, h.startsAt)
	}
	log.Infof("Done defrag %s", headers)
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
