package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"time"
)

import "github.com/robyoung/go-whisper"

func testWhisperCreate() (ts []*whisper.TimeSeriesPoint) {
	path := "/tmp/test.wsp"
	ts = make([]*whisper.TimeSeriesPoint, 30)

	os.Remove(path) // Don't care if it failes
	retentions, err := whisper.ParseRetentionDefs("1m:30m")
	if err != nil {
		panic(err)
	}
	wsp, err := whisper.Create(path, retentions, whisper.Sum, 0.5)
	if err != nil {
		panic(err)
	}
	defer wsp.Close()

	for i, _ := range ts {
		dur := fmt.Sprintf("-%dm", 29-i)
		tdur, _ := time.ParseDuration(dur)
		point := new(whisper.TimeSeriesPoint)
		point.Value = float64(rand.Intn(100))
		point.Time = int(time.Now().Add(tdur).Unix())
		ts[i] = point
		wsp.Update(point.Value, point.Time)
		log.Printf("WhisperCreate(): point -%dm is %.2f\n", 29-i, point.Value)
	}

	return ts
}

func testWhisperNulls() (ts []*whisper.TimeSeriesPoint) {
	path := "/tmp/test.wsp"
	ts = make([]*whisper.TimeSeriesPoint, 30)

	os.Remove(path) // Don't care if it fails
	retentions, err := whisper.ParseRetentionDefs("1m:30m")
	if err != nil {
		panic(err)
	}
	wsp, err := whisper.Create(path, retentions, whisper.Sum, 0.5)
	if err != nil {
		panic(err)
	}
	defer wsp.Close()

	for i, _ := range ts {
		tdur, _ := time.ParseDuration(fmt.Sprintf("-%dm", 29-i))
		j := rand.Intn(100)
		point := new(whisper.TimeSeriesPoint)
		if j < 75 {
			point.Value = math.NaN()
		} else {
			point.Value = float64(j)
		}
		point.Time = int(time.Now().Add(tdur).Unix())
		ts[i] = point
		log.Printf("WhisperNulls(): point -%dm is %.2f\n", 29-i, point.Value)
	}

	wsp.UpdateMany(ts)
	return ts
}

func testValidateWhisper(ts []*whisper.TimeSeriesPoint) error {
	path := "/tmp/test.wsp"

	wsp, err := whisper.Open(path)
	if err != nil {
		return err
	}
	defer wsp.Close()

	wspData, err := wsp.Fetch(0, int(time.Now().Unix()))
	if err != nil {
		return err
	}
	for i, v := range wspData.Values() {
		// In order time points...should match what's in data
		if v == ts[i].Value {
			continue
		} else if math.IsNaN(v) && math.IsNaN(ts[i].Value) {
			continue
		} else {
			return fmt.Errorf("Whipser point %d is %f but should be %f\n", i, v, ts[i].Value)
		}
	}

	return nil
}

func main() {
	log.Println("Initial Go Whisper testing...")
	data := testWhisperCreate()
	err := testValidateWhisper(data)
	if err != nil {
		panic(err)
	}
	log.Println("Testing for Null support...")
	data = testWhisperNulls()
	err = testValidateWhisper(data)
	if err != nil {
		panic(err)
	}
}
