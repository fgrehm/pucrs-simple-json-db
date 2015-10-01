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
	for _, val := range s1 {
		found := false
		for _, x := range s2 {
			if x == val {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
