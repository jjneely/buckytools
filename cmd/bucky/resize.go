package main

import (
	"sort"

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

	c := NewCommand(resizeCommand, "resize", usage, short, long)
	SetupCommon(c)
	SetupHostname(c)
	SetupSingle(c)

	c.Flag.StringVar(&resizeFilename, "f", "", "whisper file to resize")
	c.Flag.IntVar(&resizeArchiveIndex, "index", -1, "archive index")
	c.Flag.StringVar(&resizeNewRetention, "retention", "", "new retention")
	c.Flag.StringVar(&resizeAgg, "agg", "", "new aggregation method")
}

// resizeCommand runs this subcommand.
func resizeCommand(c Command) int {
	target, err := whisper.Open(resizeFilename)
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

	result, err := whisper.Create(resizeFilename+".new", newRetentions, aggMethod, target.XFF())
	if err != nil {
		panic(err)
	}

	newFile := result.File()
	oldFile := target.File()

	if resizeArchiveIndex != -1 {
		newArchives := result.ArchiveInfos()
		for i, oldArchive := range target.ArchiveInfos() {
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

		tarchive := newArchives[resizeArchiveIndex]
		dps := result.ReadSeries(tarchive.Offset(), int64(tarchive.End()), &tarchive)
		sort.Sort(whisper.DataPoints(dps))
		for i, dp := range dps {
			if _, err := newFile.WriteAt(dp.Bytes(), tarchive.Offset()+whisper.PointSize*int64(i)); err != nil {
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
