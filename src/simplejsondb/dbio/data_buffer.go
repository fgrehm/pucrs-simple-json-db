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
	inUse      bool
	referenced bool
	isDirty    bool
	position   int
	data       []byte
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
		frame.referenced = true
		return &DataBlock{ID: id, Data: frame.data}, nil

	}

	log.Debugf("FETCH blockID=%d, cacheHit=false", id)

	needsEvict := len(db.nextVictims) == db.size
	var err error
	if needsEvict {
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
	frame.referenced = needsEvict
	db.nextVictims = append(db.nextVictims, id)
	db.idToFrame[id] = frame

	return &DataBlock{ID: id, Data: frame.data}, nil
}

func (db *dataBuffer) MarkAsDirty(dataBlockID uint16) error {
	log.Debugf("DIRTY blockID=%d", dataBlockID)
	frame := db.idToFrame[dataBlockID]
	if frame == nil {
		panic("Tried to mark as dirty a block that is no longer on the buffer")
	}
	frame.isDirty = true
	frame.referenced = true
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
	var victimFrame *bufferFrame
	var victimID uint16

	victimPosition := 0
	for {
		victimID = db.nextVictims[victimPosition]
		victimFrame = db.idToFrame[victimID]

		if !victimFrame.referenced {
			break
		}

		log.Debugf("MARK_UNREFERENCED blockID=%d, dirty=%t", victimID, victimFrame.isDirty)
		victimFrame.referenced = false
		victimPosition = (victimPosition + 1) % db.size
	}

	log.Debugf("EVICT blockID=%d, dirty=%t", victimID, victimFrame.isDirty)
	if victimFrame.isDirty {
		if err := db.df.WriteBlock(victimID, victimFrame.data); err != nil {
			return nil, err
		}
	}

	victimFrame.inUse = false
	victimFrame.isDirty = false
	delete(db.idToFrame, victimID)
	db.nextVictims = append(db.nextVictims[victimPosition+1:], db.nextVictims[0:victimPosition]...)

	return victimFrame, nil
}
