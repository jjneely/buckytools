package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"
)

import "github.com/jjneely/carbontools/whisper"

func whisperCreate(path string) (ts []*whisper.TimeSeriesPoint) {
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

	rand.Seed(time.Now().Unix())
	for i, _ := range ts {
		dur := fmt.Sprintf("-%dm", 29-i)
		tdur, _ := time.ParseDuration(dur)
		point := new(whisper.TimeSeriesPoint)
		point.Value = float64(rand.Intn(100))
		point.Time = int(time.Now().Add(tdur).Unix())
		ts[i] = point
		log.Printf("WhisperCreate(): point -%dm is %.2f\n", 29-i, point.Value)
	}
	wsp.UpdateMany(ts)

	return ts
}

func whisperCreateNulls(path string) (ts []*whisper.TimeSeriesPoint) {
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
		if j < 65 {
			point.Value = math.NaN()
			point.Time = 0
		} else {
			point.Value = float64(j)
			point.Time = int(time.Now().Add(tdur).Unix())
		}
		ts[i] = point
		log.Printf("WhisperNulls(): point -%dm is %.2f\n", 29-i, point.Value)
		if !math.IsNaN(point.Value) {
			wsp.Update(point.Value, point.Time)
		}
	}

	return ts
}

func validateWhisper(path string, ts []*whisper.TimeSeriesPoint) error {
	wsp, err := whisper.Open(path)
	if err != nil {
		return err
	}
	defer wsp.Close()

	wspData, err := wsp.Fetch(0, int(time.Now().Unix()))
	if err != nil {
		return err
	}
	var flag error = nil
	for i, v := range wspData.Values() {
		// In order time points...should match what's in data
		if v == ts[i].Value {
			log.Printf("Verifty %.2f == %.2f\n", v, ts[i].Value)
		} else if math.IsNaN(v) && math.IsNaN(ts[i].Value) {
			log.Printf("Verifty %.2f == %.2f\n", v, ts[i].Value)
		} else {
			log.Printf("Verifty %.2f != %.2f\n", v, ts[i].Value)
			if flag == nil {
				flag = fmt.Errorf("Whipser point %d is %f but should be %f\n", i, v, ts[i].Value)
			}
		}
	}

	return flag
}

func dump(path string) {
	wsp, err := whisper.Open(path)
	if err != nil {
		panic(err)
	}
	defer wsp.Close()

	wspData, err := wsp.Fetch(0, int(time.Now().Unix()))
	if err != nil {
		panic(err)
	}

	interval := wspData.FromTime()
	for i, v := range wspData.Values() {
		log.Printf("DP[%d] == %d\t%.2f\n", i, interval, v)
		interval += wspData.Step()
	}
}

func simulateFill(a, b []*whisper.TimeSeriesPoint) []*whisper.TimeSeriesPoint {
	// Assume that we are simulating the fill operation on WSP DBs created
	// with the above functions.
	dataMerged := make([]*whisper.TimeSeriesPoint, 30)
	for i, _ := range dataMerged {
		if math.IsNaN(b[i].Value) && !math.IsNaN(a[i].Value) {
			dataMerged[i] = a[i]
		} else if !math.IsNaN(b[i].Value) {
			dataMerged[i] = b[i]
		} else {
			dataMerged[i].Value = math.NaN()
			dataMerged[i].Time = 0
		}
	}

	return dataMerged
}

// Fill() will fill data from src into dst without overwriting data currently
// in dst, and always copying the highest resulution data no matter what time
// ranges.
// * source - path to the Whisper file
// * dest - path to the Whisper file
// * startTime - Unix time such as time.Now().Unix().  We fill from this time
//   walking backwards to the begining of the retentions.
//
// This code heavily inspired by https://github.com/jssjr/carbonate
// func Fill(source, dest string, startTime int) error

func TestFill(t *testing.T) {
	dataA := whisperCreate("a.wsp")
	if validateWhisper("a.wsp", dataA) != nil {
		t.Error("Data written to a.wsp doesn't match what was read")
	}

	dataB := whisperCreateNulls("b.wsp")
	if validateWhisper("b.wsp", dataB) != nil {
		t.Error("Data with nulls written to b.wsp doesn't match what was read")
	}

	err := Fill("a.wsp", "b.wsp", int(time.Now().Unix()))
	if err != nil {
		t.Error(err)
	}
	log.Println("Final results:")
	dump("b.wsp")

	err = validateWhisper("b.wsp", simulateFill(dataA, dataB))
	if err != nil {
		t.Error(err)
	}
}

func TestReference(t *testing.T) {
	dataA := whisperCreate("a.wsp")
	dataB := whisperCreateNulls("b.wsp")

	// whisper-fill.py needs to be in the PATH somewhere
	log.Println("Running whisper-fill.py...")
	c := exec.Command("whisper-fill.py", "a.wsp", "b.wsp")
	c.Run()

	err := validateWhisper("b.wsp", simulateFill(dataA, dataB))
	if err != nil {
		t.Error(err)
	}
}
