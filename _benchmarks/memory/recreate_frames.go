package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
)

const (
	DATAFILE_SIZE  = 1024 * 1024 * 256 // 256 MB
	DATABLOCK_SIZE = 1024 * 4          // 4KB
	FRAMES_COUNT   = 512               // Positions
)

var (
	DatablockByteOrder = binary.BigEndian
)

func main() {
	// Allocate 2MB of memory (512 frames * 4KB)
	frames := make([][]byte, FRAMES_COUNT, FRAMES_COUNT)
	for i := 0; i < FRAMES_COUNT; i++ {
		frames[i] = make([]byte, DATABLOCK_SIZE, DATABLOCK_SIZE)
	}

	file, err := os.OpenFile("benchmark.dat", os.O_RDWR, 0666)
	if err != nil {
		panic(err)
	}

	dataBlocksCount := DATAFILE_SIZE / DATABLOCK_SIZE

	fmt.Printf("frames[0]: %x\n", frames[0][0:8])
	fmt.Printf("frames[1]: %x\n", frames[1][0:8])
	fmt.Printf("frames[2]: %x\n", frames[2][0:8])
	for j := 0; j < 100; j++ {
		if j%10 == 0 {
			println(j)
		}

		for i := 0; i < dataBlocksCount; i++ {
			if _, err := file.Seek(int64(i*DATABLOCK_SIZE), 0); err != nil {
				panic(err)
			}
			buffer := bytes.NewBuffer(make([]byte, 0, DATABLOCK_SIZE))

			if _, err := io.CopyN(buffer, file, DATABLOCK_SIZE); err != nil {
				panic(err)
			}
			frames[i%FRAMES_COUNT] = buffer.Bytes()
			frame := frames[i%FRAMES_COUNT]

			read := DatablockByteOrder.Uint64(frame[0:8])
			if read != uint64(i) {
				panic("Invalid block info")
			}
		}
	}
	fmt.Printf("frames[0]: %x\n", frames[0][0:8])
	fmt.Printf("frames[1]: %x\n", frames[1][0:8])
	fmt.Printf("frames[2]: %x\n", frames[2][0:8])

	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("bytes obtained from system:        %dMB\n", m.HeapSys/1024/1024)
	fmt.Printf("bytes allocated and not yet freed: %dMB\n", m.HeapAlloc/1024/1024)
	fmt.Printf("bytes in idle spans:               %dMB\n", m.HeapIdle/1024/1024)
	fmt.Printf("bytes released to the OS:          %dKB\n", m.HeapReleased/1024)
}
