package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"os"
)

const (
	DATAFILE_SIZE  = 1024 * 1024 * 256 // 256 MB
	DATABLOCK_SIZE = 1024 * 4          // 4KB
)

var (
	DatablockByteOrder = binary.BigEndian
)

type Datafile interface {
	Close()
	ReadBlock(id int) (*Datablock, error)
	WriteBlock(db *Datablock) error
}

type datafile struct {
	file *os.File
}

type Datablock struct {
	Data []byte
	ID   uint16
}

func NewDatafile(filename string) (Datafile, error) {
	file, err := openDatafile(filename)
	if err != nil {
		return nil, err
	}

	return &datafile{file: file}, nil
}

func openDatafile(filename string) (*os.File, error) {
	if _, err := os.Stat(filename); err == nil {
		log.Println("Datafile exists, reusing it")
		return os.OpenFile(filename, os.O_RDWR, 0666)
	}

	log.Println("Creating datafile...")
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	file.Truncate(DATAFILE_SIZE)
	log.Println("DONE")

	return file, nil
}

func (df *datafile) ReadBlock(id int) (*Datablock, error) {
	if _, err := df.file.Seek(int64(id*DATABLOCK_SIZE), 0); err != nil {
		return nil, err
	}
	log.Printf("Reading datablock %016d", id)
	buffer := bytes.NewBuffer(make([]byte, 0, DATABLOCK_SIZE))

	if _, err := io.CopyN(buffer, df.file, DATABLOCK_SIZE); err != nil {
		return nil, err
	}

	return &Datablock{ID: uint16(id), Data: buffer.Bytes()}, nil
}

func (df *datafile) WriteBlock(db *Datablock) error {
	if _, err := df.file.Seek(int64(db.ID*DATABLOCK_SIZE), 0); err != nil {
		return err
	}
	log.Printf("Writing datablock %016d", db.ID)
	buffer := bytes.NewBuffer(db.Data)

	if _, err := io.CopyN(df.file, buffer, DATABLOCK_SIZE); err != nil {
		return err
	}
	df.file.Sync()

	return nil
}

func (df *datafile) Close() {
	df.file.Close()
}
