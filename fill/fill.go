package fill

import (
	"math"
	"sort"
	"time"
)

import "github.com/go-graphite/buckytools/whisper"

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
		dstWsp.UpdateManyWithRetention(points, v.MaxRetention())

		stop = fromTime
		if start >= stop {
			// Nothing more to fetch
			break
		}
	}

	return nil
}

// Files() will fill data from src into dst without overwriting data currently
// in dst, and always copying the highest resulution data no matter what time
// ranges.
// * source - path to the Whisper file
// * dest - path to the Whisper file
// * startTime - Unix time such as time.Now().Unix().  We fill from this time
//   walking backwards to the begining of the retentions.
func Files(source, dest string, startTime int) error {
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

	return OpenWSP(srcWsp, dstWsp, startTime)
}

// All() is a convenience function when you need to fill all of
// the given whisper file paths rather than a specific time range.
// * source - path to source Whisper file
// * dest   - path to destination Whisper file
func All(source, dest string) error {
	return Files(source, dest, int(time.Now().Unix()))
}

// OpenWSP() runs the fill operation on two whisper.Whisper objects that are
// already open.
// * srcWsp - source *whisper.Whisper object
// * dstWsp - destination *whisper,Whisper object
// * startTime - Unix time such as int(time.Now().Unix()).  We fill from
//   this time walking backwards to the beginning.
//
// This code heavily inspired by https://github.com/jssjr/carbonate
// and matches its behavior exactly.
func OpenWSP(srcWsp, dstWsp *whisper.Whisper, startTime int) error {
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
				// Carbonate ignores single units lost.  Means:
				// XXX: Gap of a single step are ignored as the
				// following if uses > not, =>
				if (start - gapstart) > v.SecondsPerPoint() {
					// XXX: Fence post: This replaces the
					// current DP -- a known good value
					fillArchive(srcWsp, dstWsp, gapstart-ts.Step(), start)
					// We always fill starting at gap-step
					// because the Fetch() command will pull
					// the next valid interval's point even
					// if we give it a valid interval.
				}
				gapstart = -1
			} else if gapstart >= 0 && start == ts.UntilTime()-ts.Step() {
				// The timeSeries doesn't actually include a
				// value for ts.UntilTime(), like len() we need
				// to subtract a step to index the last value
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
