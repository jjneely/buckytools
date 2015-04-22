package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
)

var metricsCache *MetricsCacheType

func logRequest(r *http.Request) {
	log.Printf("%s - - %s %s", r.RemoteAddr, r.Method, r.RequestURI)
}

func unmarshalList(encoded string) ([]string, error) {
	data := make([]string, 0)
	err := json.Unmarshal([]byte(encoded), &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func main() {
	var bindAddress string

	flag.StringVar(&bindAddress, "bind", "0.0.0.0:4242",
		"IP:PORT to listen for HTTP requests.")
	flag.StringVar(&bindAddress, "b", "0.0.0.0:4242",
		"IP:PORT to listen for HTTP requests.")
	flag.Parse()

	http.HandleFunc("/", http.NotFound)
	http.HandleFunc("/metrics", listMetrics)
	http.HandleFunc("/metrics/", serveMetrics)

	log.Printf("Starting server on %s", bindAddress)
	err := http.ListenAndServe(bindAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
