package main

import (
	"encoding/json"
	"log"
	"net/http"
)

// listHashring encodes the hashing members in JSON and sends them to the
// client.
func listHashring(w http.ResponseWriter, r *http.Request) {
	logRequest(r)
	if r.Method != "GET" {
		http.Error(w, "Bad Request.", http.StatusBadRequest)
		return
	}

	blob, err := json.Marshal(hashring)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		log.Printf("Error marshalling data: %s", err)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write(blob)
	}
}
