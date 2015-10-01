package core

import (
	"errors"
)

type DataBuffer interface {
	FetchBlock(id uint16) (*Datablock, error)
	WithBlock(id uint16, withFunc func(*Datablock) error) error
	Flush() error
}

type dataBuffer struct {
	df       Datafile
	frames   map[uint16]*bufferFrame
	frameIds []uint16
	size     int
}

type bufferFrame struct {
	dataBlock *Datablock
}

func NewDataBuffer(df Datafile, size int) DataBuffer {
	return &dataBuffer{
		df:       df,
		size:     size,
		frames:   make(map[uint16]*bufferFrame),
		frameIds: make([]uint16, 0, size),
	}
}

func (db *dataBuffer) FetchBlock(id uint16) (*Datablock, error) {
	if db.frames[id] != nil {
		return db.frames[id].dataBlock, nil
	} else {
		if len(db.frameIds) == db.size {
			db.evictFirstFrame()
		}

		dataBlock, err := db.df.ReadBlock(id)
		if err != nil {
			return nil, err
		}
		db.frames[dataBlock.ID] = &bufferFrame{dataBlock}
		db.frameIds = append(db.frameIds, dataBlock.ID)

		return dataBlock, nil
	}
}

// This is a method that deals with reading and writing datablocks back into the buffer,
// soon to be used when manipulating blocks concurrently
func (db *dataBuffer) WithBlock(id uint16, withFunc func(*Datablock) error) error {
	block, err := db.FetchBlock(id)
	if err != nil {
		return err
	} else {
		return withFunc(block)
	}
}

func (db *dataBuffer) Flush() error {
	return errors.New("Not implemented yet")
}

func (db *dataBuffer) evictFirstFrame() {
	id := db.frames[db.frameIds[0]].dataBlock.ID
	// log.Printf("Removing %d from data buffer", id)
	delete(db.frames, id)
	db.frameIds = db.frameIds[1:]
}
