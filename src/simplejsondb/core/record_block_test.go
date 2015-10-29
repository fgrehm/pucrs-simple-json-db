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

	localID := rb.Add(uint32(10), []byte("01234567890123456789"))

	// "Force reload" the wrapper
	rb = core.NewRecordBlock(block)

	// Did the utilization increase?
	if rb.Utilization()-prevUtilization != core.RECORD_HEADER_SIZE+20 {
		t.Errorf("Allocated an unexpected amount of data, got %d, expected %d, total %d", rb.Utilization()-prevUtilization, core.RECORD_HEADER_SIZE+20, rb.Utilization())
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
		t.Errorf("Freed an unexpected amount of data, got %d, expected %d, total %d", rb.Utilization()-prevUtilization, core.RECORD_HEADER_SIZE, rb.Utilization())
	}
}

func TestRecordBlock_Allocation(t *testing.T) {
	block := &dbio.DataBlock{Data: make([]byte, dbio.DATABLOCK_SIZE)}
	rb := core.NewRecordBlock(block)

	rb.Add(uint32(10), []byte("0123456789"))
	localID := rb.Add(uint32(11), []byte("AAAAAAAAAA"))
	rb.Add(uint32(12), []byte("9999999999"))

	if err := rb.Remove(localID); err != nil {
		t.Fatal(err)
	}
	prevUtilization := rb.Utilization()
	newLocalID := rb.Add(uint32(13), []byte("00"))
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
	// Ensure things are persisted as they should
	if string(block.Data[0:22]) != "0123456789999999999900" {
		t.Errorf("Did not defrag the block. Block contents `%s`, expected `%s`", string(block.Data[0:92]), "0123456789999999999900")
	}

	// Ensure this will change
	prevUtilization = rb.Utilization()

	// Add yet another set of bytes
	rb.Add(uint32(14), []byte("NNNNNNNNNN"))

	// "Force reload" the wrapper
	rb = core.NewRecordBlock(block)

	// Did we update the utilization accordingly?
	if rb.Utilization()-prevUtilization != core.RECORD_HEADER_SIZE+10 {
		t.Errorf("Allocated an unexpected amount of data (allocated %d bytes, expected %d, total %d)", rb.Utilization()-prevUtilization, core.RECORD_HEADER_SIZE+10, rb.Utilization())
	}

	// Can we still read the record that filled in the gap?
	data, err = rb.ReadRecordData(localID)
	if err != nil {
		t.Fatal(err)
	}
	if data != "00" {
		t.Errorf("Unexpected data found `%s`, expected `%s`", data, "00")
	}

	// Last but not least, ensure things are persisted as they should
	if string(block.Data[0:32]) != "0123456789999999999900NNNNNNNNNN" {
		t.Errorf("Invalid final state `%s`, expected `%s`", string(block.Data[0:32]), "0123456789999999999900NNNNNNNNNN")
	}
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
