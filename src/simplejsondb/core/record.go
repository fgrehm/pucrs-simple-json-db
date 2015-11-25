package core

import "encoding/json"

type Record struct {
	ID   uint32
	Data []byte
	parsedJSON map[string]interface{}
}

func (r *Record) ParseJSON() (map[string]interface{}, error) {
	if r.parsedJSON != nil {
		return r.parsedJSON, nil
	}

	r.parsedJSON = map[string]interface{}{}
	if err := json.Unmarshal(r.Data, &r.parsedJSON); err != nil {
		return nil, err
	}
	return r.parsedJSON, nil
}

type RowID struct {
	DataBlockID uint16
	LocalID     uint16
}
