package main

func main() {
	df, err := NewDatafile("metadata-db.dat")
	if err != nil {
		panic(err)
	}
	defer df.Close()

	df.WriteInt16(0, 7)
	df.WriteInt16(2, 1)

	var i int64 = 0
	for ; i < 10; i++ {
		i, err := df.ReadInt16(i)
		if err != nil {
			panic(err)
		}

		println(i)
	}
}
