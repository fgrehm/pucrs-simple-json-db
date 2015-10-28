package core_test

import (
	"simplejsondb/core"
	"simplejsondb/dbio"

	"testing"
)

func TestRecordBlock_BasicAddReadAndDeleteFlow(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := core.NewRecordBlock(block)

	prevUtilization := rb.Utilization()

	_, localID := rb.Add(uint32(10), []byte("01234567890123456789"))

	// "Force reload" the wrapper
	rb = core.NewRecordBlock(block)

	// Did the utilization increase?
	if rb.Utilization()-prevUtilization != core.RECORD_HEADER_SIZE+20 {
		t.Errorf("Allocated an unexpected amount of data")
	}

	// Can we read the record again?
	data, err := rb.ReadRecordData(localID)
	if err != nil {
		t.Fatal(err)
	}

	if data != "01234567890123456789" {
		t.Errorf("Unexpected data found `%s`", data)
	}

	if err := rb.Remove(localID); err != nil {
		t.Fatal(err)
	}

	// "Force reload" the wrapper
	rb = core.NewRecordBlock(block)
	if _, err := rb.ReadRecordData(localID); err == nil {
		t.Error("Did not return an error")
	}

	if rb.Utilization()-prevUtilization != core.RECORD_HEADER_SIZE {
		t.Error("Freed an unexpected amount of data")
		println(rb.Utilization()-prevUtilization)
	}
}

func TestRecordBlock_MakesUseOfEmptySlots(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := core.NewRecordBlock(block)

	rb.Add(uint32(10), []byte("0123456789"))
	_, localID := rb.Add(uint32(11), []byte("AAAAAAAAAA"))
	rb.Add(uint32(12), []byte("9999999999"))

	if err := rb.Remove(localID); err != nil {
		t.Fatal(err)
	}
	_, newLocalID := rb.Add(uint32(13), []byte("00"))
	if localID != newLocalID {
		t.Error("Did not reuse the deleted record localid")
	}

	// "Force reload" the wrapper
	rb = core.NewRecordBlock(block)

	// Can we read the record again?
	data, err := rb.ReadRecordData(localID)
	if err != nil {
		t.Fatal(err)
	}
	if data != "00" {
		t.Errorf("Unexpected data found `%s`", data)
	}

	// Last but not least, ensure things are persisted as they should
	if string(block.Data[0:32]) != "012345678900AAAAAAAA9999999999" {
		t.Errorf("Did not reuse the free spot. Block contents `%s`", string(block.Data[0:32]))
	}
}

func TestRecordBlock_ReturnsTheAmountOfBytesWritten(t *testing.T) {
	t.Fatal("TODO")

	// Force a chained row
	// test that a record that fits return all bytes
}

func TestRecordBlock_NextBlockID(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := core.NewRecordBlock(block)

	if rb.NextBlockID() != 0 {
		t.Fatal("Invalid next block ID found")
	}
	rb.SetNextBlockID(99)

	// "Force reload" the wrapper
	rb = core.NewRecordBlock(block)
	if rb.NextBlockID() != 99 {
		t.Fatal("Invalid next block ID found")
	}
}

func TestRecordBlock_PrevBlockID(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := core.NewRecordBlock(block)

	if rb.PrevBlockID() != 0 {
		t.Fatal("Invalid next block ID found")
	}
	rb.SetPrevBlockID(99)

	// "Force reload" the wrapper
	rb = core.NewRecordBlock(block)
	if rb.PrevBlockID() != 99 {
		t.Fatal("Invalid next block ID found")
	}
}
