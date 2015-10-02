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
	df          Datafile
	frames      []*bufferFrame          // Reusable frames of memory
	idToFrame   map[uint16]*bufferFrame // Used for mapping an id to a buffer on the frames array
	nextVictims []uint16
	size        int
}

type bufferFrame struct {
	inUse    bool
	position int
	data     []byte
}

func NewDataBuffer(df Datafile, size int) DataBuffer {
	// Reusable array of buffers
	frames := make([]*bufferFrame, 0, size)
	for i := 0; i < size; i++ {
		frames = append(frames, &bufferFrame{
			inUse:    false,
			position: i,
			data:     make([]byte, DATABLOCK_SIZE, DATABLOCK_SIZE),
		})
	}

	return &dataBuffer{
		df:          df,
		size:        size,
		frames:      frames,
		idToFrame:   make(map[uint16]*bufferFrame),
		nextVictims: make([]uint16, 0, size),
	}
}

func (db *dataBuffer) FetchBlock(id uint16) (*Datablock, error) {
	frame, present := db.idToFrame[id]
	if present {
		return &Datablock{ID: id, Data: frame.data}, nil
	} else {
		if len(db.nextVictims) == db.size {
			db.evictOldestFrame()
		}

		for i := 0; i < db.size; i++ {
			frame = db.frames[i]
			if !db.frames[i].inUse {
				break
			}
		}

		err := db.df.ReadBlock(id, frame.data)
		if err != nil {
			return nil, err
		}

		frame.inUse = true
		db.nextVictims = append(db.nextVictims, id)
		db.idToFrame[id] = frame

		return &Datablock{ID: id, Data: frame.data}, nil
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

func (db *dataBuffer) evictOldestFrame() {
	id := db.nextVictims[0]
	frame := db.idToFrame[id]

	frame.inUse = false
	delete(db.idToFrame, id)
	db.nextVictims = db.nextVictims[1:]
}
