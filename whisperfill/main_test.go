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

func whisperCreateData(path string, ts []*whisper.TimeSeriesPoint) error {
	os.Remove(path) // Don't care if it fails
	retentions, err := whisper.ParseRetentionDefs("1m:30m")
	if err != nil {
		return err
	}
	wsp, err := whisper.Create(path, retentions, whisper.Sum, 0.5)
	if err != nil {
		return err
	}
	defer wsp.Close()

	// Iterate through the slice so we can support null values
	for _, point := range ts {
		if math.IsNaN(point.Value) {
			continue
		}
		err = wsp.Update(point.Value, point.Time)
		if err != nil {
			return err
		}
	}

	return nil
}

func whisperCreate(path string) ([]*whisper.TimeSeriesPoint, error) {
	ts := make([]*whisper.TimeSeriesPoint, 30)

	os.Remove(path) // Don't care if it fails
	retentions, err := whisper.ParseRetentionDefs("1m:30m")
	if err != nil {
		return ts, err
	}
	wsp, err := whisper.Create(path, retentions, whisper.Sum, 0.5)
	if err != nil {
		return ts, err
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
		//log.Printf("WhisperCreate(): point -%dm is %.2f\n", 29-i, point.Value)
	}
	wsp.UpdateMany(ts)

	return ts, nil
}

func whisperCreateNulls(path string) ([]*whisper.TimeSeriesPoint, error) {
	ts := make([]*whisper.TimeSeriesPoint, 30)

	os.Remove(path) // Don't care if it fails
	retentions, err := whisper.ParseRetentionDefs("1m:30m")
	if err != nil {
		return ts, err
	}
	wsp, err := whisper.Create(path, retentions, whisper.Sum, 0.5)
	if err != nil {
		return ts, err
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
		//log.Printf("WhisperNulls(): point -%dm is %.2f\n", 29-i, point.Value)
		if !math.IsNaN(point.Value) {
			err = wsp.Update(point.Value, point.Time)
			if err != nil {
				return ts, err
			}
		}
	}

	return ts, nil
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
			//log.Printf("Verifty %.2f == %.2f\n", v, ts[i].Value)
		} else if math.IsNaN(v) && math.IsNaN(ts[i].Value) {
			//log.Printf("Verifty %.2f == %.2f\n", v, ts[i].Value)
		} else {
			//log.Printf("Verifty %.2f != %.2f\n", v, ts[i].Value)
			if flag == nil {
				flag = fmt.Errorf("Whipser point %d is %f but should be %f\n", i, v, ts[i].Value)
			}
		}
	}

	return flag
}

func fetchFromFile(path string) ([]*whisper.TimeSeriesPoint, error) {
	// Init the datastructure we will load values into
	tsp := make([]*whisper.TimeSeriesPoint, 30)
	for i, _ := range tsp {
		point := new(whisper.TimeSeriesPoint)
		point.Value = math.NaN()
		tsp[i] = point
	}

	// Try to open the file
	wsp, err := whisper.Open(path)
	if err != nil {
		return tsp, err
	}
	defer wsp.Close()

	// Parse and fetch data from it
	ts, err := wsp.Fetch(0, int(time.Now().Unix()))
	if err != nil {
		return tsp, err
	}

	return ts.Points(), nil
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
	// This is a shallow copy operation.
	dataMerged := make([]*whisper.TimeSeriesPoint, 30)
	// copy everything in b over to our return value
	copy(dataMerged, b)
	gapstart := -1
	for i, v := range dataMerged {
		if math.IsNaN(v.Value) && gapstart < 0 {
			gapstart = i
		} else if !math.IsNaN(v.Value) && gapstart >= 0 {
			if i-gapstart > 1 {
				// like the source, ignore single null values.
				// like the source copy over the current value.
				copy(dataMerged[gapstart:i+1], a[gapstart:i+1])
			}
			gapstart = -1
		} else if gapstart >= 0 && i == len(dataMerged)-1 {
			copy(dataMerged[gapstart:i+1], a[gapstart:i+1])
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
	dataA, err := whisperCreate("a.wsp")
	if err != nil {
		t.Fatal(err)
	}
	if validateWhisper("a.wsp", dataA) != nil {
		t.Error("Data written to a.wsp doesn't match what was read")
	}

	dataB, err := whisperCreateNulls("b.wsp")
	if err != nil {
		t.Fatal(err)
	}
	if validateWhisper("b.wsp", dataB) != nil {
		t.Error("Data with nulls written to b.wsp doesn't match what was read")
	}

	err = Fill("a.wsp", "b.wsp", int(time.Now().Unix()))
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
	// Create our random test data
	dataA, err := whisperCreate("a1.wsp")
	if err != nil {
		t.Fatal(err)
	}
	dataB, err := whisperCreateNulls("b1.wsp")
	if err != nil {
		t.Fatal(err)
	}

	// Create an identical set of test data
	err = whisperCreateData("a2.wsp", dataA)
	if err != nil {
		t.Fatal(err)
	}
	err = whisperCreateData("b2.wsp", dataB)
	if err != nil {
		t.Fatal(err)
	}

	// Check our copies
	err = validateWhisper("a2.wsp", dataA)
	if err != nil {
		t.Fatal(err)
	}
	err = validateWhisper("b2.wsp", dataB)
	if err != nil {
		t.Fatal(err)
	}

	// whisper-fill.py needs to be in the PATH somewhere
	log.Println("Running whisper-fill.py...")
	c := exec.Command("whisper-fill.py", "a1.wsp", "b1.wsp")
	err = c.Run()
	if err != nil {
		t.Error(err)
	}
	pythonFill, err := fetchFromFile("b1.wsp")
	if err != nil {
		t.Error(err)
	}
	// Here, pythonFill is either NaNs (failed to read WSP file) or
	// the data from the python reference fill operation

	// Run my version
	err = Fill("a2.wsp", "b2.wsp", int(time.Now().Unix()))
	if err != nil {
		t.Error(err)
	}
	goFill, err := fetchFromFile("b2.wsp")
	if err != nil {
		t.Error(err)
	}

	// Compare to what we think our version should be
	simuFill := simulateFill(dataA, dataB)
	err = validateWhisper("b2.wsp", simuFill)
	if err != nil {
		t.Error(err)
	}

	// Now try to print out a table of A, B, Python, Go, Simu
	fmt.Printf("A     \tB     \tPython\tGo    \tSimu\n")
	fmt.Printf("======\t======\t======\t======\t======\n")
	for i := 0; i < 30; i++ {
		fmt.Printf("%6.1f\t%6.1f\t%6.1f\t%6.1f\t%6.1f\n", dataA[i].Value, dataB[i].Value, pythonFill[i].Value, goFill[i].Value, simuFill[i].Value)
	}
}
