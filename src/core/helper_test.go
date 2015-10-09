package core_test

// slicesEqual accepts two slices and returns a boolean
// indicating whether they are equal.
// Intentionally not implementing a sort, so this is
// a bit brute-force, but the amount of test data is small.
//
// Note: This function DOES return a false positive for a sample
// such as []int{1, 1, 2} == []int{1, 2, 2} because the bitmap can
// not have duplicate values.
func slicesEqual(s1, s2 []byte) bool {
	if len(s1) != len(s2) {
		return false
	}
	for i, val := range s1 {
		if s2[i] != val {
			return false
		}
	}
	return true
}

// An in memory data file does what it says and allows us to avoid hitting
// the FS during tests
type inMemoryDataFile struct {
	blocks         [][]byte
	closeFunc      func() error
	readBlockFunc  func(uint16, []byte) error
	writeBlockFunc func(uint16, []byte) error
}

func newFakeDataFile(blocks [][]byte) *inMemoryDataFile {
	return &inMemoryDataFile{
		blocks: blocks,
		closeFunc: func() error {
			return nil // NOOP by default
		},
		writeBlockFunc: func(id uint16, data []byte) error {
			block := blocks[id]
			for i := range block {
				block[i] = data[i]
			}
			return nil // NOOP by default
		},
		readBlockFunc: func(id uint16, data []byte) error {
			block := blocks[id]
			for i := 0; i < len(block); i++ {
				data[i] = block[i]
			}
			return nil
		},
	}
}

func (df *inMemoryDataFile) Close() error {
	return df.closeFunc()
}
func (df *inMemoryDataFile) ReadBlock(id uint16, data []byte) error {
	return df.readBlockFunc(id, data)
}
func (df *inMemoryDataFile) WriteBlock(id uint16, data []byte) error {
	return df.writeBlockFunc(id, data)
}
