package test_utils

// slicesEqual accepts two slices and returns a boolean
// indicating whether they are equal.
// Intentionally not implementing a sort, so this is
// a bit brute-force, but the amount of test data is small.
//
// Note: This function DOES return a false positive for a sample
// such as []int{1, 1, 2} == []int{1, 2, 2} because the bitmap can
// not have duplicate values.
func SlicesEqual(s1, s2 []byte) bool {
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
type InMemoryDataFile struct {
	Blocks         [][]byte
	CloseFunc      func() error
	ReadBlockFunc  func(uint16, []byte) error
	WriteBlockFunc func(uint16, []byte) error
}

func NewFakeDataFile(blocks [][]byte) *InMemoryDataFile {
	return &InMemoryDataFile{
		Blocks: blocks,
		CloseFunc: func() error {
			return nil // NOOP by default
		},
		WriteBlockFunc: func(id uint16, data []byte) error {
			block := blocks[id]
			for i := range block {
				block[i] = data[i]
			}
			return nil // NOOP by default
		},
		ReadBlockFunc: func(id uint16, data []byte) error {
			block := blocks[id]
			for i := 0; i < len(block); i++ {
				data[i] = block[i]
			}
			return nil
		},
	}
}

func (df *InMemoryDataFile) Close() error {
	return df.CloseFunc()
}
func (df *InMemoryDataFile) ReadBlock(id uint16, data []byte) error {
	return df.ReadBlockFunc(id, data)
}
func (df *InMemoryDataFile) WriteBlock(id uint16, data []byte) error {
	return df.WriteBlockFunc(id, data)
}
