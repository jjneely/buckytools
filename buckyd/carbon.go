package main

import (
	"bytes"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"time"
)

type TimeSeriesPoint struct {
	Metric    string
	Timestamp int64
	Value     float64
}

func runCarbonServer(bind string) {
	cache := runCache()
	carbon := carbonServer(bind)

	for m := range carbon {
		cache <- m
	}
}

// carbonServer starts a TCP listener and handles incoming Graphite line
// protocol data.
func carbonServer(bind string) chan *TimeSeriesPoint {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill)
	tcpAddr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		log.Fatal(err)
	}
	ln, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Fatalf("Error listening for TCP carbon connections: %s", err)
	}

	c := make(chan *TimeSeriesPoint)
	go func() {
		defer ln.Close()
		defer close(c)
		for {
			select {
			case <-sig:
				log.Printf("Received signal, shutting down carbon server")
				return
			default:
			}
			ln.SetDeadline(time.Now().Add(time.Second))
			conn, err := ln.Accept()
			if err, ok := err.(net.Error); ok && err.Timeout() {
				// Deadline timeout, continue loop
				continue
			}
			if err != nil {
				log.Printf("Accepting TCP connection failed: %s", err)
				continue
			}

			go handleCarbon(conn, c)
		}
	}()

	return c
}

func handleCarbon(conn net.Conn, c chan *TimeSeriesPoint) {
	// Each connection has a 1KiB buffer for reading / parsing incoming data
	buf := make([]byte, 1024, 1024)
	offset := 0

	defer conn.Close()
	for {
		n, err := conn.Read(buf[offset:])
		if n > 0 {
			lines := bytes.Split(buf[:n], []byte{'\n'})
			last := len(lines) - 1
			for i, line := range lines {
				if i == last && err != io.EOF {
					copy(buf, line)
					offset = len(line)
				} else {
					dp := parseCarbonLine(line)
					if dp != nil {
						c <- dp
					}
				}
			}
		}
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Printf("Error reading TCP connection: %s", err)
			return
		}
	}
}

func parseCarbonLine(buf []byte) *TimeSeriesPoint {
	// XXX: Sanity check the metric name, or do we let that happen at
	// the relay level?
	var i int64
	var f float64
	var err error

	fields := bytes.Split(buf, []byte{' '})
	if len(fields) != 3 {
		log.Printf("Illegal metric: %s", string(buf))
		return nil
	}

	dp := new(TimeSeriesPoint)
	dp.Metric = string(fields[0])
	i, err = strconv.ParseInt(string(fields[1]), 10, 64)
	if err != nil {
		f, err = strconv.ParseFloat(string(fields[1]), 64)
		i = int64(f)
	}
	if err != nil {
		log.Printf("Illegal metric: %s", string(buf))
		return nil
	}
	dp.Timestamp = i
	f, err = strconv.ParseFloat(string(fields[2]), 64)
	if err != nil {
		log.Printf("Illegal metric: %s", string(buf))
		return nil
	}
	dp.Value = f

	return dp
}
