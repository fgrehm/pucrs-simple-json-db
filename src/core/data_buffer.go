package core

import (
	"errors"
)

type DataBuffer interface {
	FetchBlock(id uint16) (*Datablock, error)
	MarkAsDirty(id uint16) error
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
	isDirty  bool
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
			if err := db.evictOldestFrame(); err != nil {
				return nil, err
			}
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

func (db *dataBuffer) MarkAsDirty(id uint16) error {
	frame := db.idToFrame[id]
	frame.isDirty = true
	return nil
}

func (db *dataBuffer) Flush() error {
	return errors.New("Not implemented yet")
}

func (db *dataBuffer) evictOldestFrame() error {
	id := db.nextVictims[0]
	frame := db.idToFrame[id]

	if frame.isDirty {
		if err := db.df.WriteBlock(id, frame.data); err != nil {
			return err
		}
	}

	frame.inUse = false
	frame.isDirty = false
	delete(db.idToFrame, id)
	db.nextVictims = db.nextVictims[1:]

	return nil
}
