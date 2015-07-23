// bucky-pickle-relay is simply designed to decode the Python Pickle
// objects used in the Graphite Pickle protocol and forward them as
// plaintext protocol metrics to an upstream carbon-relay.  In my case
// I've used carbon-c-relay for routing and hashing but most of the
// incoming data is encoded in the pickle format.
//
// Jack Neely <jjneely@42lines.net>
// 7/23/2015
package main

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

import pickle "github.com/kisielk/og-rek"

// Where we listen for incoming TCP connections
var bindTo string

// Upstream carbon-relay location to send plaintext protocol metrics
var carbonRelay string

// Debug boolean
var debug bool

// timeout connections after X seconds of blocking / inactivity
var timeout int

// maxPickleSize is the largest pickle data stream we will accept
var maxPickleSize int

// queue for decoded metrics read to send
var queue = make([]string, 0)

func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [options] upstream-relay:port\n\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "The first argument must be the network location to forward\n")
	fmt.Fprintf(os.Stderr, "plaintext carbon metrics to.\n\n")
	flag.PrintDefaults()
	os.Exit(1)
}

func serveForever() {
	log.Printf("Starting bucky-pickle-relay on %s", bindTo)
	ln, err := net.Listen("tcp", bindTo)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %s", err)
			continue
		}

		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	if debug {
		log.Printf("Connection from %s", conn.RemoteAddr().String())
	}
	defer conn.Close()

	var size int
	var sizeBuf = make([]byte, 4)
	var dataBuf = make([]byte, 0, maxPickleSize)

	for {
		conn.SetDeadline(time.Now().Add(time.Second * time.Duration(timeout)))

		// Pickle is preceded by an unsigned long integer of 4 bytes (!L)
		n, err := conn.Read(sizeBuf)
		if err == io.EOF {
			// Remote end closed connection
			return
		} else if neterr, ok := err.(net.Error); ok && neterr.Timeout() {
			// Timeout waiting for data on connection
			log.Printf("Timeout waiting for data on connection")
			return
		} else if err != nil {
			log.Printf("Error reading connection: %s", err)
			return
		}
		if n != 4 {
			log.Printf("Protocol error: Count not read 4 byte header, got %d instead", n)
		}
		size = int(binary.BigEndian.Uint32(sizeBuf))

		if size > maxPickleSize {
			log.Printf("%s attempting to send %d bytes and is too large, aborting",
				conn.RemoteAddr().String(), size)
			return
		}

		// Adjust size of dataBuf slice to not read in extra data
		dataBuf = dataBuf[0:size]
		// Now read the pickle data
		n, err = conn.Read(dataBuf)
		if err != nil {
			log.Printf("Error reading pickle: %s", err)
			return
		}
		if n != size {
			log.Printf("Protocol error: pickle data not correct size")
			return
		}

		decoder := pickle.NewDecoder(bytes.NewBuffer(dataBuf))
		object, err := decoder.Decode()
		if err != nil {
			log.Printf("Error decoding pickle: %s", err)
		}
		handlePickle(object)
	}
}

func handlePickle(object interface{}) {
	// Is this a slice -- it should be
	slice, ok := object.([]interface{})
	if !ok {
		log.Printf("Pickle object should be []interface{} and is not")
		return
	}

	for _, v := range slice {
		var key, ts, dp string
		metric, ok := v.([]interface{})
		if !ok {
			log.Printf("[]interface{} not data type inside pickle slice")
			continue
		}

		key, ok = metric[0].(string)
		if !ok {
			log.Printf("Unexpected type where metric key string should be")
			continue
		}

		datatuple, ok := metric[1].([]interface{})
		if !ok {
			log.Printf("ts, dp []interface{} not found")
			continue
		}

		switch t := datatuple[0].(type) {
		default:
			log.Printf("Unexpected type in pickle data")
			continue
		case string:
			ts = strings.TrimSpace(t)
		case int64:
			ts = fmt.Sprintf("%d", t)
		}

		switch t := datatuple[1].(type) {
		default:
			log.Printf("Unexpected type in pickle data")
			continue
		case string:
			dp = strings.TrimSpace(t)
		case int64:
			dp = fmt.Sprintf("%d", t)
		case float64:
			dp = fmt.Sprintf("%.12f", t)
		}

		fmt.Printf("Discovered metric: %s %s %s\n", key, dp, ts)
	}
}

func main() {
	flag.StringVar(&bindTo, "b", ":2004",
		"Address to bind to for incoming connections.")
	flag.BoolVar(&debug, "d", false,
		"Debug mode.")
	flag.IntVar(&timeout, "t", 30,
		"Connection block and idle timeout.")
	flag.IntVar(&maxPickleSize, "x", 1*1024*1024,
		"Buffer size for incoming pickle data and max size of each pickle.")
	flag.Parse()
	if flag.NArg() != 1 {
		usage()
	}

	carbonRelay = flag.Arg(0)
	log.Printf("bucky-pickle-relay Copyright 2015 42 Lines, Inc.")
	serveForever()
}
