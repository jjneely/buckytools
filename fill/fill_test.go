package fill

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/go-graphite/go-whisper"
)

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

	now := int(time.Now().Unix())
	fromTime := now - wsp.Retentions()[0].MaxRetention()
	wspData, err := wsp.Fetch(fromTime, now)
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

	now := int(time.Now().Unix())
	fromTime := now - wsp.Retentions()[0].MaxRetention()

	// Parse and fetch data from it
	ts, err := wsp.Fetch(fromTime, now)
	if err != nil {
		return tsp, err
	}

	return ts.PointPointers(), nil
}

func simulateFill(a, b []*whisper.TimeSeriesPoint) []*whisper.TimeSeriesPoint {
	// Assume that we are simulating the fill operation on WSP DBs created
	// with the above functions.
	// This is a shallow copy operation.
	dataMerged := make([]*whisper.TimeSeriesPoint, len(b))
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

// Files() will fill data from src into dst without overwriting data currently
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

	err = Files("a.wsp", "b.wsp", int(time.Now().Unix()))
	if err != nil {
		t.Error(err)
	}

	simuFill := simulateFill(dataA, dataB)
	err = validateWhisper("b.wsp", simuFill)
	if err != nil {
		t.Error(err)
	}

	result, err := fetchFromFile("b.wsp")
	if err != nil {
		// Couldn't read from file?
		t.Fatal(err)
	}

	// Table of actual results
	fmt.Printf("A     \tB     \tSimu\tResult\n")
	fmt.Printf("======\t======\t======\t======\n")
	for i := 0; i < 30; i++ {
		fmt.Printf("%6.1f\t%6.1f\t%6.1f\t%6.1f\n", dataA[i].Value, dataB[i].Value, simuFill[i].Value, result[i].Value)
	}
	fmt.Println()
}

func whisperCreateDataMany(path string, ts []*whisper.TimeSeriesPoint) error {
	os.Remove(path) // Don't care if it fails
	retentions, err := whisper.ParseRetentionDefs("1m:30m,5m:60m,15m:150m")
	if err != nil {
		return err
	}
	wsp, err := whisper.Create(path, retentions, whisper.Sum, 0)
	if err != nil {
		return err
	}
	defer wsp.Close()

	// Iterate through the slice so we can support null values
	wsp.UpdateMany(ts)

	return nil
}

func whisperCreateNullsManyArchives(path string) ([]*whisper.TimeSeriesPoint, error) {
	values := []float64{
		math.NaN(),
		math.NaN(),
		math.NaN(),
		0.0,
		7.0,
		1.0,
		7.0,
		2.0,
		9.0,
		9.0,
		9.0,
		4.0,
		3.0,
		2.0,
		7.0,
		5.0,
		1.0,
		9.0,
		4.0,
		4.0,
		1.0,
		5.0,
		5.0,
		8.0,
		4.0,
		2.0,
		6.0,
		0.0,
		math.NaN(),
		math.NaN(),
	}

	now := int(time.Now().Unix())
	ts := make([]*whisper.TimeSeriesPoint, 0, len(values))
	for _, v := range values {
		ts = append(ts, &whisper.TimeSeriesPoint{Value: v, Time: now})
		now -= 60
	}

	os.Remove(path) // Don't care if it fails
	retentions, err := whisper.ParseRetentionDefs("1m:30m,5m:60m,15m:150m")
	if err != nil {
		return ts, err
	}
	wsp, err := whisper.Create(path, retentions, whisper.Sum, 0)
	if err != nil {
		return ts, err
	}
	defer wsp.Close()

	for _, point := range ts {
		_ = wsp.Update(point.Value, point.Time)
	}

	return ts, nil
}

func TestTwoArchives(t *testing.T) {
	dataC, err := whisperCreateNullsManyArchives("c1.wsp")
	if err != nil {
		t.Fatal(err)
	}

	// Create an identical set of test data
	err = whisperCreateDataMany("c2.wsp", dataC)
	if err != nil {
		t.Fatal(err)
	}

	err = whisperCreateDataMany("d1.wsp", dataC)
	if err != nil {
		t.Fatal(err)
	}

	err = whisperCreateDataMany("d2.wsp", dataC)
	if err != nil {
		t.Fatal(err)
	}

	// whisper-fill.py needs to be in the PATH somewhere
	log.Println("Running whisper-fill.py...")
	c := exec.Command("whisper-fill.py", "c1.wsp", "d1.wsp")

	reference_err := c.Run()
	pythonFill, err := fetchFromFile("d1.wsp")

	// Run my version
	err = Files("c2.wsp", "d2.wsp", int(time.Now().Unix()))
	if err != nil {
		t.Error(err)
	}
	goFill, err := fetchFromFile("d2.wsp")
	if err != nil {
		t.Error(err)
	}

	// Compare to what we think our version should be
	simuFill := simulateFill(dataC, dataC)

	err = validateWhisper("d2.wsp", simuFill)
	if err != nil {
		t.Error(err)
	}

	// Validate the reference if whisper-fill.py was found
	if reference_err == nil {
		err = validateWhisper("d1.wsp", simuFill)
		if err != nil {
			t.Error(err)
		}
	}

	if len(goFill) != len(pythonFill) {
		t.Fatalf("length mismatch, python=%v, go=%v", len(goFill), len(pythonFill))
	}

	// Now try to print out a table of C, D, Python, Go, Simu
	fmt.Printf("C     \tD     \tPython\tGo    \tSimu\n")
	fmt.Printf("======\t======\t======\t======\t======\n")
	for i := 0; i < len(goFill); i++ {
		if reference_err != nil {
			fmt.Printf("%6.1f\t%6.1f\t%6.1f\t%6.1f\t%6.1f\n", dataC[i].Value, dataC[i].Value, math.NaN(), goFill[i].Value, simuFill[i].Value)
		} else {
			fmt.Printf("%6.1f\t%6.1f\t%6.1f\t%6.1f\t%6.1f\n", dataC[i].Value, dataC[i].Value, pythonFill[i].Value, goFill[i].Value, simuFill[i].Value)
		}
	}
	fmt.Println()
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
	reference_err := c.Run()
	pythonFill, err := fetchFromFile("b1.wsp")

	// Run my version
	err = Files("a2.wsp", "b2.wsp", int(time.Now().Unix()))
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

	// Validate the reference if whisper-fill.py was found
	if reference_err == nil {
		err = validateWhisper("b1.wsp", simuFill)
		if err != nil {
			t.Error(err)
		}
	}

	// Now try to print out a table of A, B, Python, Go, Simu
	fmt.Printf("A     \tB     \tPython\tGo    \tSimu\n")
	fmt.Printf("======\t======\t======\t======\t======\n")
	for i := 0; i < 30; i++ {
		if reference_err != nil {
			fmt.Printf("%6.1f\t%6.1f\t%6.1f\t%6.1f\t%6.1f\n", dataA[i].Value, dataB[i].Value, math.NaN(), goFill[i].Value, simuFill[i].Value)
		} else {
			fmt.Printf("%6.1f\t%6.1f\t%6.1f\t%6.1f\t%6.1f\n", dataA[i].Value, dataB[i].Value, pythonFill[i].Value, goFill[i].Value, simuFill[i].Value)
		}
	}
	fmt.Println()
}
