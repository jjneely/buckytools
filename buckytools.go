package buckytools

import (
	"encoding/json"
)

const (
	// Buckytools suite version
	Version = "0.1.3"
)

// MetricStatType A JSON marshalable FileInfo type
type MetricStatType struct {
	Name    string // Filename
	Size    int64  // file size
	Mode    uint32 // mode bits
	ModTime int64  // Unix time
}

// JSONRingType is a datastructure that identifies the name of the server
// buckdy is running on and contains a slice of nodes which are
// "server:instance" (where ":instance" is optional) formatted strings
type JSONRingType struct {
	Name  string
	Nodes []string
}

func (j *JSONRingType) String() string {
	blob, err := json.Marshal(j)
	if err != nil {
		return err.Error()
	}
	return string(blob)
}
