package buckytools

import (
	"encoding/json"
)

const (
	// Buckytools suite version
	Version = "0.1.9"
)

// SupportedHashTypes is the string identifiers of the hashing algorithms
// used for the consistent hash ring.  This slice must be sorted.
var SupportedHashTypes = []string{
	"carbon",
	"jump_fnv1a",
}

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
	Name     string
	Nodes    []string
	Algo     string
	Replicas int
}

func (j *JSONRingType) String() string {
	blob, err := json.Marshal(j)
	if err != nil {
		return err.Error()
	}
	return string(blob)
}
