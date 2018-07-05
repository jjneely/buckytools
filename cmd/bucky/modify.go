package main

import (
	"io"
	"os"
	"sort"
	"syscall"

	"github.com/go-graphite/buckytools/whisper"
)

var (
	resizeFilename     string
	resizeArchiveIndex int
	resizeNewRetention string
	resizeAgg          string
)

func init() {
	usage := ""
	short := ""
	long := ``

	c := NewCommand(modifyCommand, "modify", usage, short, long)
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)

	c.Flag.StringVar(&resizeFilename, "f", "", "whisper file to resize")
	c.Flag.IntVar(&resizeArchiveIndex, "index", -1, "archive index")
	c.Flag.StringVar(&resizeNewRetention, "retention", "", "new retention")
	c.Flag.StringVar(&resizeAgg, "agg", "", "new aggregation method")
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

	newFile := result.File()
	oldFile := target.File()

	if resizeArchiveIndex != -1 {
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

		startPoint := dps[0]
		for _, dp := range dps {
			if dp.Interval() > 0 && dp.Interval() < startPoint.Interval() {
				startPoint = dp
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
			extendedStart := startPoint.Interval() - (newArchive.NumberOfPoints()-oldArchive.NumberOfPoints())*newArchive.SecondsPerPoint()
			extendedEnd := startPoint.Interval()

			// println("extendedStart", extendedStart)
			// println("extendedEnd", extendedEnd)

			fromInterval := lowerArchive.Interval(extendedStart)
			untilInterval := lowerArchive.Interval(extendedEnd)
			baseInterval := result.GetBaseInterval(&lowerArchive)
			fromOffset := lowerArchive.PointOffset(baseInterval, fromInterval) - whisper.PointSize
			untilOffset := lowerArchive.PointOffset(baseInterval, untilInterval) + whisper.PointSize

			lowerDps := result.ReadSeries(fromOffset, untilOffset, &lowerArchive)
			// pretty.Println(lowerDps)
			lowerIntervalMap := map[int]float64{}
			for _, dp := range lowerDps {
				lowerIntervalMap[dp.Interval()] = dp.Value()
			}
			// startInterval = 0
			for ts := extendedStart; ts < extendedEnd; ts += newArchive.SecondsPerPoint() {
				val, ok := lowerIntervalMap[lowerArchive.Interval(ts)-lowerArchive.SecondsPerPoint()]
				// println(ts, lowerArchive.Interval(ts))
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
			startInterval = startPoint.Interval()
			newFile.WriteAt(startPoint.Bytes(), newArchive.Offset())
		} else {
			newFile.WriteAt(startPoint.Bytes(), newArchive.PointOffset(startInterval, startPoint.Interval()))
		}

		// pretty.Println(lowerArchive)

		for _, dp := range dps {
			if dp.Interval() == 0 || dp.Interval() == startInterval {
				continue
			}
			// if _, err := newFile.WriteAt(dp.Bytes(), newArchive.Offset()+whisper.PointSize*int64(i)); err != nil {
			// 	panic(err)
			// }
			// println(dp.Interval(), dp.Value())
			if _, err := newFile.WriteAt(dp.Bytes(), newArchive.PointOffset(startInterval, dp.Interval())); err != nil {
				panic(err)
			}
		}
	} else {
		oldArchives := target.ArchiveInfos()
		sort.Slice(oldArchives, func(i, j int) bool {
			return oldArchives[i].SecondsPerPoint() < oldArchives[j].SecondsPerPoint()
		})
		newArchives := result.ArchiveInfos()
		sort.Slice(newArchives, func(i, j int) bool {
			return newArchives[i].SecondsPerPoint() < newArchives[j].SecondsPerPoint()
		})

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

	result.Close()

	return 0
}
