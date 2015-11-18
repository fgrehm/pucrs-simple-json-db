package dbio

import (
	log "github.com/Sirupsen/logrus"
)

type DataBuffer interface {
	FetchBlock(id uint16) (*DataBlock, error)
	MarkAsDirty(id uint16) error
	Sync() error
}

type dataBuffer struct {
	df          DataFile
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

func NewDataBuffer(df DataFile, size int) DataBuffer {
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

func (db *dataBuffer) FetchBlock(id uint16) (*DataBlock, error) {
	frame, present := db.idToFrame[id]
	if present {
		log.Debugf("FETCH blockID=%d, cacheHit=true", id)

		return &DataBlock{ID: id, Data: frame.data}, nil
	} else {
		log.Debugf("FETCH blockID=%d, cacheHit=false", id)

		var frame *bufferFrame
		var err error
		if len(db.nextVictims) == db.size {
			if frame, err = db.evictFrame(); err != nil {
				return nil, err
			}
		} else {
			for i := 0; i < db.size; i++ {
				frame = db.frames[i]
				if !db.frames[i].inUse {
					break
				}
			}
		}

		err = db.df.ReadBlock(id, frame.data)
		if err != nil {
			return nil, err
		}

		frame.inUse = true
		db.nextVictims = append(db.nextVictims, id)
		db.idToFrame[id] = frame

		return &DataBlock{ID: id, Data: frame.data}, nil
	}
}

func (db *dataBuffer) MarkAsDirty(dataBlockID uint16) error {
	log.Debugf("DIRTY blockID=%d", dataBlockID)
	frame := db.idToFrame[dataBlockID]
	if frame == nil {
		panic("Tried to mark as dirty a block that is no longer on the buffer")
	}
	frame.isDirty = true
	return nil
}

func (db *dataBuffer) Sync() error {
	for dataBlockID, frame := range db.idToFrame {
		if !frame.isDirty {
			continue
		}

		if err := db.df.WriteBlock(dataBlockID, frame.data); err != nil {
			return err
		}

		frame.isDirty = false
	}
	return nil
}

func (db *dataBuffer) evictFrame() (*bufferFrame, error) {
	id, frame := db.pickFrameToEvict()
	log.Debugf("EVICT blockID=%d, dirty=%t", id, frame.isDirty)
	if frame.isDirty {
		if err := db.df.WriteBlock(id, frame.data); err != nil {
			return nil, err
		}
	}

	frame.inUse = false
	frame.isDirty = false
	delete(db.idToFrame, id)
	db.nextVictims = db.nextVictims[1:]

	return frame, nil
}

func (db *dataBuffer) pickFrameToEvict() (uint16, *bufferFrame) {
	id := db.nextVictims[0]
	return id, db.idToFrame[id]
}
