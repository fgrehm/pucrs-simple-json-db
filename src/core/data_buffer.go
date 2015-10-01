package core

import (
	"errors"
)

type DataBuffer interface {
	FetchBlock(id uint16) (*Datablock, error)
	Flush() error
}

type dataBuffer struct {
	df Datafile
}

func NewDataBuffer(df Datafile) DataBuffer {
	return &dataBuffer{df}
}

func (db *dataBuffer) FetchBlock(id uint16) (*Datablock, error) {
	return db.df.ReadBlock(id)
}

func (db *dataBuffer) Flush() error {
	return errors.New("Not implemented yet")
}
