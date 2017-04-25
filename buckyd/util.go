package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
)

import "github.com/golang/snappy"

// copySparse copies a file like interface src to the file dst.  Any 4KiB
// chunk of null bytes in the src will become a sparse hole in the dst file.
// Therefore, this converts files to sparse files on disk.
//
// Returned are the number of bytes written (in the apparent since, not
// actual) and any error that occurred.
//
// This is otherwise very similar to io.Copy()
func copySparse(dst *os.File, src io.Reader) (written int64, err error) {
	buf := make([]byte, 4*1024)   // work in 4k chunks
	zeros := make([]byte, 4*1024) // reference slice of zeros
	size := int64(0)
	for {
		nr, er := src.Read(buf)
		size += int64(nr)
		if bytes.Equal(zeros[:nr], buf[:nr]) {
			// a block of zeros, seek
			_, err = dst.Seek(int64(nr), 1)
			if err != nil {
				break
			}
			written += int64(nr)
		} else if nr > 0 {
			nw, ew := dst.Write(buf[0:nr])
			if nw > 0 {
				written += int64(nw)
			}
			if ew != nil {
				err = ew
				break
			}
			if nr != nw {
				err = io.ErrShortWrite
				break
			}
		}
		if er == io.EOF {
			// Success, set the file size
			dst.Truncate(size)
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	return written, err
}

// copySnappy takes an io.Reader interface and returns a bytes.Buffer
// containing the data read from the Reader compressed via the Goolge
// Snappy algorithm.  Errors reading, if any, are returned.  An io.EOF
// error is success for us and we return a nil error.
func copySnappy(src io.Reader) (dst *bytes.Buffer, err error) {
	buf := make([]byte, 4*1024) // work in 4k chunks
	dst = new(bytes.Buffer)
	writer := snappy.NewBufferedWriter(dst)
	for {
		nr, er := src.Read(buf)
		if nr > 0 {
			_, ew := writer.Write(buf[0:nr])
			if ew != nil {
				err = ew
				break
			}
		}
		if er == io.EOF {
			break
		}
		if er != nil {
			err = er
			break
		}
	}
	writer.Close()
	return dst, err
}

// logRequest logs an incoming HTTP request.
func logRequest(r *http.Request) {
	log.Printf("%s - - %s %s", r.RemoteAddr, r.Method, r.RequestURI)
}

// unmarshalList is a common function for unmarshalling an incoming JSON
// list object for processing as part of a REST call.
func unmarshalList(encoded string) ([]string, error) {
	data := make([]string, 0)
	err := json.Unmarshal([]byte(encoded), &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}
