package kvstore

import "encoding/json"

type snapFormat struct {
	Data map[string]string `json:"data"`
}

func marshalSnap(data map[string]string) ([]byte, error) {
	return json.Marshal(snapFormat{Data: data})
}

func unmarshalSnap(b []byte) (map[string]string, error) {
	var s snapFormat
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	if s.Data == nil {
		s.Data = make(map[string]string)
	}
	return s.Data, nil
}
