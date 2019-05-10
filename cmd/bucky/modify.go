package main

import (
	"io"
	"os"
	"sort"
	"syscall"
	"time"

	// TODO: migrate to github.com/go-graphite/go-whisper
	"github.com/go-graphite/buckytools/whisper"
)

var (
	resizeFilename     string
	resizeArchiveIndex int
	resizeNewRetention string
	resizeAgg          string
	resizeUseNow       bool
)

func init() {
	usage := "[options]"
	short := "Resize archive or change aggregation policy."
	long := `Modify command supports two operations: resize, or update aggregation policy.

Resize mode allows user to resize one archive at a time. It only change the
targeting archive and does not affect other archives. Use -index to specify
resized archives.

Use -retention to specify new policy (with the same format in whisper
configuration). To resize to bigger time range, modify command upsample data
from lower-resolution archives.

Example:
	bucky modify -index 1 -retention 1m:30d -f 100_olddata.wsp

Change aggregation policy. Other than changing policy, this command would also
try to correct data if it's changing policy from average -> sum, or sum -> average.
For other types of changes, it would only do a simple data copy.

Example:
	bucky modify -f small.wsp.new -agg average

By default, both tool would copy the original whisper file as a new back in the
same location.
`

	c := NewCommand(modifyCommand, "modify", usage, short, long)
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)

	c.Flag.StringVar(&resizeFilename, "f", "", "whisper file to modify")
	c.Flag.IntVar(&resizeArchiveIndex, "index", -1, "archive index")
	c.Flag.StringVar(&resizeNewRetention, "retention", "", "new retention policy (with the same format in whisper configuration; e.g. 1m:30d)")
	c.Flag.StringVar(&resizeAgg, "agg", "", "new aggregation method")
	c.Flag.BoolVar(&resizeUseNow, "now", false, "use now to find archive offset")
}

// modifyCommand runs this subcommand.
func modifyCommand(c Command) int {
	resizeFile, err := os.Open(resizeFilename)
	if err != nil {
		panic(err)
	}
	if err = syscall.Flock(int(resizeFile.Fd()), syscall.LOCK_EX); err != nil {
		resizeFile.Close()
		panic(err)
	}
	defer resizeFile.Close()

	backupFile, err := os.Create(resizeFilename + ".bak")
	if err != nil {
		panic(err)
	}
	if _, err := io.Copy(backupFile, resizeFile); err != nil {
		panic(err)
	}
	if err := backupFile.Close(); err != nil {
		panic(err)
	}
	if err := resizeFile.Truncate(0); err != nil {
		panic(err)
	}
	if _, err := resizeFile.Seek(0, 0); err != nil {
		panic(err)
	}

	target, err := whisper.Open(resizeFilename + ".bak")
	if err != nil {
		panic(err)
	}

	newRetentions := target.Retentions()
	if resizeArchiveIndex != -1 {
		retention, err := whisper.ParseRetentionDef(resizeNewRetention)
		if err != nil {
			panic(err)
		}
		newRetentions[resizeArchiveIndex] = retention
	}

	aggMethod := target.AggregationMethod()
	if resizeAgg != "" {
		switch resizeAgg {
		case "average":
			aggMethod = whisper.Average
		case "sum":
			aggMethod = whisper.Sum
		default:
			panic("unsupported aggregation method")
		}
	}

	result, err := whisper.Create2(resizeFile, newRetentions, aggMethod, target.XFF())
	if err != nil {
		panic(err)
	}

	if resizeArchiveIndex != -1 {
		resizeArchive(target, result)
	} else {
		modifyAgg(target, result, aggMethod)
	}

	result.Close()

	return 0
}

func modifyAgg(target, result *whisper.Whisper, aggMethod whisper.AggregationMethod) {
	oldArchives := target.ArchiveInfos()
	sort.Slice(oldArchives, func(i, j int) bool {
		return oldArchives[i].SecondsPerPoint() < oldArchives[j].SecondsPerPoint()
	})
	newArchives := result.ArchiveInfos()
	sort.Slice(newArchives, func(i, j int) bool {
		return newArchives[i].SecondsPerPoint() < newArchives[j].SecondsPerPoint()
	})
	newFile := result.File()
	for i, oldArchive := range oldArchives {
		dps := target.ReadSeries(oldArchive.Offset(), int64(oldArchive.End()), &oldArchive)
		for j, dp := range dps {
			if i > 0 {
				if target.AggregationMethod() == whisper.Average && aggMethod == whisper.Sum {
					dp = whisper.NewDataPoint(dp.Interval(), dp.Value()*float64(oldArchives[i].SecondsPerPoint()/oldArchives[i-1].SecondsPerPoint()))
				} else if target.AggregationMethod() == whisper.Sum && aggMethod == whisper.Average {
					dp = whisper.NewDataPoint(dp.Interval(), dp.Value()/float64(oldArchives[i].SecondsPerPoint()/oldArchives[i-1].SecondsPerPoint()))
				}
			}

			if _, err := newFile.WriteAt(dp.Bytes(), newArchives[i].Offset()+whisper.PointSize*int64(j)); err != nil {
				panic(err)
			}
		}
	}
}

func resizeArchive(target, result *whisper.Whisper) {
	newFile := result.File()
	oldFile := target.File()
	newArchives := result.ArchiveInfos()
	oldArchives := target.ArchiveInfos()
	for i, oldArchive := range oldArchives {
		if _, err := oldFile.Seek(oldArchive.Offset(), 0); err != nil {
			panic(err)
		}
		body := make([]byte, oldArchive.Size())
		if _, err := oldFile.Read(body); err != nil {
			panic(err)
		}
		if _, err := newFile.WriteAt(body, newArchives[i].Offset()); err != nil {
			panic(err)
		}
	}

	oldArchive := oldArchives[resizeArchiveIndex]
	dps := target.ReadSeries(oldArchive.Offset(), int64(oldArchive.End()), &oldArchive)

	oldStartPoint := dps[0]
	for _, dp := range dps {
		if dp.Interval() > 0 && dp.Interval() < oldStartPoint.Interval() {
			oldStartPoint = dp
		}
	}

	startInterval := 0
	newArchive := newArchives[resizeArchiveIndex]

	// best-effort backfilling of extended dps
	{
		lowerArchive := newArchive
		for _, arc := range newArchives {
			if arc.SecondsPerPoint() > newArchive.SecondsPerPoint() && (lowerArchive.SecondsPerPoint() == newArchive.SecondsPerPoint() || lowerArchive.SecondsPerPoint() > arc.SecondsPerPoint()) {
				lowerArchive = arc
			}
		}
		offsetInterval := oldStartPoint.Interval()
		if resizeUseNow {
			offsetInterval = int(time.Now().Add(time.Duration(-1 * int64(newArchive.MaxRetention()))).Unix())
		}

		extendedStart := offsetInterval - (newArchive.NumberOfPoints()-oldArchive.NumberOfPoints())*newArchive.SecondsPerPoint()
		extendedEnd := offsetInterval

		fromInterval := lowerArchive.Interval(extendedStart)
		untilInterval := lowerArchive.Interval(extendedEnd)
		baseInterval := result.GetBaseInterval(&lowerArchive)
		fromOffset := lowerArchive.PointOffset(baseInterval, fromInterval) - whisper.PointSize
		untilOffset := lowerArchive.PointOffset(baseInterval, untilInterval) + whisper.PointSize

		lowerDps := result.ReadSeries(fromOffset, untilOffset, &lowerArchive)
		lowerIntervalMap := map[int]float64{}
		for _, dp := range lowerDps {
			lowerIntervalMap[dp.Interval()] = dp.Value()
		}
		for ts := extendedStart; ts < extendedEnd; ts += newArchive.SecondsPerPoint() {
			val, ok := lowerIntervalMap[lowerArchive.Interval(ts)-lowerArchive.SecondsPerPoint()]
			if !ok {
				continue
			}

			switch result.AggregationMethod() {
			case whisper.Sum:
				val /= float64(lowerArchive.SecondsPerPoint() / newArchive.SecondsPerPoint())
			}

			dp := whisper.NewDataPoint(ts, val)
			// println(dp.Interval(), val)
			if startInterval == 0 {
				startInterval = ts
				newFile.WriteAt(dp.Bytes(), newArchive.Offset())
			} else {
				newFile.WriteAt(dp.Bytes(), newArchive.PointOffset(startInterval, dp.Interval()))
			}
		}
	}

	if startInterval == 0 {
		startInterval = oldStartPoint.Interval()
		newFile.WriteAt(oldStartPoint.Bytes(), newArchive.Offset())
	} else {
		newFile.WriteAt(oldStartPoint.Bytes(), newArchive.PointOffset(startInterval, oldStartPoint.Interval()))
	}

	for _, dp := range dps {
		if dp.Interval() == 0 || dp.Interval() == startInterval {
			continue
		}

		if _, err := newFile.WriteAt(dp.Bytes(), newArchive.PointOffset(startInterval, dp.Interval())); err != nil {
			panic(err)
		}
	}
}
