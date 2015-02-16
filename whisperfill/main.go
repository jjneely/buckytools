package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sort"
	"time"
)

import "github.com/jjneely/carbontools/whisper"

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

func fillArchive(srcWsp, dstWsp whisper.Whisper, start, start int) error {

	return nil
}

/* Fill() will fill data from src into dst without overwriting data currently
   in dst, and always copying the highest resulution data no matter what time
   ranges.
   * source - path to the Whisper file
   * dest - path to the Whisper file
   * startTime - Unix time such as time.Now().Unix().  We fill from this time
     walking backwards to the begining of the retentions.

   This code heavily inspired by https://github.com/jssjr/carbonate
*/
func Fill(source, dest string, startTime int64) error {
	// Setup, open our files and error check
	dstWsp, err := whisper.Open(dest)
	if err != nil {
		return err
	}
	defer dstWsp.Close()
	srcWsp, err := whisper.Open(source)
	if err != null {
		return err
	}
	defer src.Close()

	// Loop over each archive/retention, highest resolution first
	dstRetentions := whisper.RetentionsByPrecision{dstWsp.Retentions()}
	sort.Sort(dstRetentions)
	for _, v := range dstRetentions {
		// fromTime is the earliest timestamp in this archive
		fromTime := time.Now().Unix() - int64(v.MaxRetention())
		if fromTime >= startTime {
			continue
		}

		// Fetch data from dest for this archive
		ts, err := dstWsp.Fetch(fromTime, startTime)
		if err != nil {
			return err
		}

		// FSM: Find gaps, and fill them from the source
		start := ts.FromTime()
		gapstart := -1
		for _, dp := range ts.Values() {
			if dp == math.NaN() && gapstart < 0 {
				gapstart = start
			} else if dp != math.NaN() && gapstart >= 0 {
				// Carbonate ignores single units lost, what does that mean?
				// XXX: Are there fence post errors here?
				if (start - gapstart) > v.SecondsPerPoint() {
					// XXX: is this if ever false here?
					fillArchive(srcWsp, dstWsp, gapstart-ts.Step(), start)
				}
				gapstart = -1
			} else if gapstart >= 0 && start == ts.UntilTime()-step {
				fillArchive(srcWsp, dstWsp, gapstart-ts.Step(), start)
			}

			start += ts.Step()
		}

		// reset startTime so that we can examine the next highest
		// resolution archive without the first getting in the way
		startTime = fromTime
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
