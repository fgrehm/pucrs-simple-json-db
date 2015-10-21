package core

type RecordFinder interface {
	Find(rowID RowID) (*Record, error)
}

type recordFinder struct {
	buffer DataBuffer
}

func newRecordFinder(buffer DataBuffer) RecordFinder {
	return &recordFinder{buffer}
}

func (rf *recordFinder) Find(rowID RowID) (*Record, error) {
	block, err := rf.buffer.FetchBlock(rowID.DataBlockID)
	if err != nil {
		return nil, err
	}
	rba := newRecordBlockAdapter(block)

	// TODO: Deal with chained rows, BTree and the like
	data := rba.ReadRecordData(rowID.LocalID)
	return &Record{ID: rowID.RecordID, Data: data}, nil
}