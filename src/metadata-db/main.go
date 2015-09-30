package main

import (
	"log"
)

func main() {
	df, err := NewDatafile("metadata-db.dat")
	if err != nil {
		panic(err)
	}
	defer df.Close()

	block, err := df.ReadBlock(5)
	if err != nil {
		panic(err)
	}
	log.Printf("%x", block.Data[0])
	block.Data[0] = 0x08
	log.Printf("%x", block.Data[1])
	block.Data[1] = 0x09

	df.WriteBlock(block)
}
