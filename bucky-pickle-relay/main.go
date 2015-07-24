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

import pickle "github.com/jjneely/buckytools/pickle"

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
var queue chan []string

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
			// Yield execution, we can't accept new connections anyway
			time.Sleep(time.Second)
			continue
		}

		go handleConn(conn)
	}
}

func readSlice(conn net.Conn, buf []byte) error {
	for read := 0; read < len(buf); {
		n, err := conn.Read(buf[read:])
		read = read + n
		if err != nil {
			return err
		}
	}

	return nil
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
		err := readSlice(conn, sizeBuf)
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
		size = int(binary.BigEndian.Uint32(sizeBuf))

		if size > maxPickleSize {
			log.Printf("%s attempting to send %d bytes and is too large, aborting",
				conn.RemoteAddr().String(), size)
			return
		}

		// Adjust size of dataBuf slice to not read in extra data
		dataBuf = dataBuf[0:size]
		// Now read the pickle data
		err = readSlice(conn, dataBuf)
		if err != nil {
			log.Printf("Error reading pickle: %s", err)
			return
		}

		decoder := pickle.NewDecoder(bytes.NewBuffer(dataBuf))
		object, err := decoder.Decode()
		if err != nil {
			log.Printf("Error decoding pickle: %s", err)
		}

		// This can block under load, we've got the goods so lets continue
		// the dialog with the client who should close the connection
		go handlePickle(object)
	}
}

func handlePickle(object interface{}) {
	metrics := make([]string, 0)
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

		metrics = append(metrics, fmt.Sprintf("%s %s %s", key, dp, ts))
	}

	queue <- metrics
}

// plainTextBatch take a slice of individual plaintext metric strings
// and returns a slice of strings with several metrics concatenated
// together with a newline separator.  This helps us send fewer larger
// packets to our target.
func plainTextBatch(metrics []string, size int) []string {
	var i int
	ret := make([]string, 0)

	for i = 0; i < len(metrics)-size; i = i + size {
		ret = append(ret, strings.Join(metrics[i:i+size], "\n"))
	}

	ret = append(ret, strings.Join(metrics[i:], "\n"))
	return ret
}

func plainTextOut() {
	var batch []string

	// We attempt to connect forever
	for {
		conn, err := net.Dial("tcp", carbonRelay)
		if err != nil {
			log.Printf("Error connecting to upstream %s: %s", carbonRelay, err)
			time.Sleep(time.Second)
			continue
		}

		for err == nil {
			// grab a pickle off the queue if we aren't re-sending
			if len(batch) == 0 {
				batch = plainTextBatch(<-queue, 10)
			}
			conn.SetDeadline(time.Now().Add(time.Second * time.Duration(timeout)))
			_, err = conn.Write([]byte(batch[0] + "\n"))
			if err == nil {
				// on err we want to resend the last packet/batch
				batch = batch[1:]
			}
		}

		log.Printf("Reconnecting to %s on error: %s", carbonRelay, err)
		conn.Close()
		time.Sleep(time.Second)
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
		"Maximum pickle size accepted.")
	pickleQueueSize := flag.Int("q", 1024,
		"Queue size for processed pickles ready to be sent.")
	flag.Parse()
	if flag.NArg() != 1 {
		usage()
	}

	log.Printf("bucky-pickle-relay Copyright 2015 42 Lines, Inc.")
	carbonRelay = flag.Arg(0)
	queue = make(chan []string, *pickleQueueSize)
	go plainTextOut()
	serveForever()
}
