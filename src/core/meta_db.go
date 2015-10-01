package core

type MetaDB interface {
	InsertRecord(data string) (uint64, error)
	FindRecord(id uint64) (*Record, error)
}

func NewMetaDB(datafilePath string) (MetaDB, error) {
	return nil, nil
}
