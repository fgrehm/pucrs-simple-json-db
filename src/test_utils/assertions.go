package test_utils

// slicesEqual accepts two slices and returns a boolean
// indicating whether they are equal.
// Intentionally not implementing a sort, so this is
// a bit brute-force, but the amount of test data is small.
//
// Note: This function DOES return a false positive for a sample
// such as []int{1, 1, 2} == []int{1, 2, 2} because the bitmap can
// not have duplicate values.
//
// XXX: This was copied from a project that I can remember =/
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
