package core

const BUFFER_SIZE = 256

type MetaDB interface {
	InsertRecord(data string) (uint64, error)
	Close() error
	// FindRecord(id uint64) (*Record, error)
	// SearchFor(key, value string) ([]*Record, error)
}

type metaDb struct {
	dataFile Datafile
	buffer   DataBuffer
}

func NewMetaDB(datafilePath string) (MetaDB, error) {
	df, err := newDatafile(datafilePath)
	if err != nil {
		return nil, err
	}
	dataBuffer := NewDataBuffer(df, BUFFER_SIZE)
	return &metaDb{df, dataBuffer}, nil
}

func (m *metaDb) InsertRecord(data string) (uint64, error) {
	// Find out if data fits in a block in advance (chained rows will come later)
	// Find out the next available datablock
	//   Read datablock zero, find out the first block has space available for insertion
	// Assign an ID and increment it (and flag the corresponding datablock that stores the ID as dirty on buffer)

	block, err := m.buffer.FetchBlock(0)
	if err != nil {
		return 0, err
	}

	for index, char := range []byte(data) {
		block.Data[index] = char
	}

	m.buffer.MarkAsDirty(0)

	return 99999, nil
}

func (m *metaDb) Close() error {
	if err := m.buffer.Sync(); err != nil {
		return err
	}
	return m.dataFile.Close()
}
