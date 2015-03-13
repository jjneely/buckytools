package main

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"sort"
	"time"
)

import "github.com/jjneely/carbontools/whisper"

func testWhisperCreate(path string) (ts []*whisper.TimeSeriesPoint) {
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

func testWhisperNulls(path string) (ts []*whisper.TimeSeriesPoint) {
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

func testValidateWhisper(path string, ts []*whisper.TimeSeriesPoint) error {
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

func dump_old(path string) {
	wsp, err := whisper.Open(path)
	if err != nil {
		panic(err)
	}
	defer wsp.Close()

	wspData, err := wsp.Fetch(0, int(time.Now().Unix()))
	if err != nil {
		panic(err)
	}

	for i, v := range wspData.Values() {
		log.Printf("DP[%d] == %.2f\n", i, v)
	}
}

// fillArchive() is a private function that fills data points from srcWSP
// into dstWsp.  Used by FIll()
// * srcWsp and dstWsp are *whisper.Whisper open files
// * start and stop define an inclusive time window to fill
// On error an error value is returned.
//
// This code heavily inspired by https://github.com/jssjr/carbonate
func fillArchive(srcWsp, dstWsp *whisper.Whisper, start, stop int) error {
	// Fetch the range defined by start and stop always taking the values
	// from the highest precision archive, which man require multiple
	// fetch/merge updates.
	srcRetentions := whisper.RetentionsByPrecision{srcWsp.Retentions()}
	sort.Sort(srcRetentions)

	if start < srcWsp.StartTime() && stop < srcWsp.StartTime() {
		// Nothing to fill/merge
		return nil
	}

	// Begin our backwards walk in time
	for _, v := range srcRetentions.Iterator() {
		points := make([]*whisper.TimeSeriesPoint, 0)
		rTime := int(time.Now().Unix()) - v.MaxRetention()
		if stop <= rTime {
			// This archive contains no data points in the window
			continue
		}

		// Start and the start time or the beginning of this archive
		fromTime := start
		if rTime > start {
			fromTime = rTime
		}

		ts, err := srcWsp.Fetch(fromTime, stop)
		if err != nil {
			return err
		}
		// Build a list of points to merge
		tsStart := ts.FromTime()
		for _, dp := range ts.Values() {
			if !math.IsNaN(dp) {
				points = append(points, &whisper.TimeSeriesPoint{tsStart, dp})
			}
			tsStart += ts.Step()
		}
		dstWsp.UpdateMany(points)

		stop = fromTime
		if start >= stop {
			// Nothing more to fetch
			break
		}
	}

	return nil
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
func Fill(source, dest string, startTime int) error {
	// Setup, open our files and error check
	dstWsp, err := whisper.Open(dest)
	if err != nil {
		return err
	}
	defer dstWsp.Close()
	srcWsp, err := whisper.Open(source)
	if err != nil {
		return err
	}
	defer srcWsp.Close()

	// Loop over each archive/retention, highest resolution first
	dstRetentions := whisper.RetentionsByPrecision{dstWsp.Retentions()}
	sort.Sort(dstRetentions)
	for _, v := range dstRetentions.Iterator() {
		// fromTime is the earliest timestamp in this archive
		fromTime := int(time.Now().Unix()) - v.MaxRetention()
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
			if math.IsNaN(dp) && gapstart < 0 {
				gapstart = start
			} else if !math.IsNaN(dp) && gapstart >= 0 {
				// Carbonate ignores single units lost, what does that mean?
				// XXX: Are there fence post errors here?
				if (start - gapstart) > v.SecondsPerPoint() {
					// XXX: is this if ever false here?
					fillArchive(srcWsp, dstWsp, gapstart-ts.Step(), start)
				}
				gapstart = -1
			} else if gapstart >= 0 && start == ts.UntilTime()-ts.Step() {
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

/*
func main() {
	dump("test-empty.wsp")
}
*/

func main() {
	log.Println("Initial Go Whisper testing...")
	dataA := testWhisperCreate("a.wsp")
	err := testValidateWhisper("a.wsp", dataA)
	if err != nil {
		panic(err)
	}
	log.Println("Testing for Null support...")
	dataB := testWhisperNulls("b.wsp")
	err = testValidateWhisper("b.wsp", dataB)
	if err != nil {
		panic(err)
	}
	c := exec.Command("cp", "-f", "a.wsp", "a.safe.wsp")
	c.Run()
	c = exec.Command("cp", "-f", "b.wsp", "b.safe.wsp")
	c.Run()

	log.Println("Testing backfill...")

	dataMerged := make([]*whisper.TimeSeriesPoint, 30)
	for i, _ := range dataMerged {
		if math.IsNaN(dataB[i].Value) && !math.IsNaN(dataA[i].Value) {
			dataMerged[i] = dataA[i]
		} else if !math.IsNaN(dataB[i].Value) {
			dataMerged[i] = dataB[i]
		} else {
			dataMerged[i].Value = math.NaN()
			dataMerged[i].Time = dataA[i].Time
		}
		log.Println(dataMerged[i].Value)
	}
	err = Fill("a.wsp", "b.wsp", int(time.Now().Unix()))
	if err != nil {
		panic(err)
	}
	err = testValidateWhisper("b.wsp", dataMerged)

	if err != nil {
		panic(err)
	}
	log.Println("Tests complete.")
}
