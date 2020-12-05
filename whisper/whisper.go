/*
	Package whisper implements Graphite's Whisper database format
*/
package whisper

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	IntSize         = 4
	FloatSize       = 4
	Float64Size     = 8
	PointSize       = 12
	MetadataSize    = 16
	ArchiveInfoSize = 12
)

const (
	Seconds = 1
	Minutes = 60
	Hours   = 3600
	Days    = 86400
	Weeks   = 86400 * 7
	Years   = 86400 * 365
)

type AggregationMethod int

const (
	Average AggregationMethod = iota + 1
	Sum
	Last
	Max
	Min
)

func unitMultiplier(s string) (int, error) {
	switch {
	case strings.HasPrefix(s, "s"):
		return Seconds, nil
	case strings.HasPrefix(s, "m"):
		return Minutes, nil
	case strings.HasPrefix(s, "h"):
		return Hours, nil
	case strings.HasPrefix(s, "d"):
		return Days, nil
	case strings.HasPrefix(s, "w"):
		return Weeks, nil
	case strings.HasPrefix(s, "y"):
		return Years, nil
	}
	return 0, fmt.Errorf("Invalid unit multiplier [%v]", s)
}

var retentionRegexp *regexp.Regexp = regexp.MustCompile("^(\\d+)([smhdwy]+)$")

func parseRetentionPart(retentionPart string) (int, error) {
	part, err := strconv.ParseInt(retentionPart, 10, 32)
	if err == nil {
		return int(part), nil
	}
	if !retentionRegexp.MatchString(retentionPart) {
		return 0, fmt.Errorf("%v", retentionPart)
	}
	matches := retentionRegexp.FindStringSubmatch(retentionPart)
	value, err := strconv.ParseInt(matches[1], 10, 32)
	if err != nil {
		panic(fmt.Sprintf("Regex on %v is borked, %v cannot be parsed as int", retentionPart, matches[1]))
	}
	multiplier, err := unitMultiplier(matches[2])
	return multiplier * int(value), err
}

/*
  Parse a retention definition as you would find in the storage-schemas.conf of a Carbon install.
  Note that this only parses a single retention definition, if you have multiple definitions (separated by a comma)
  you will have to split them yourself.

  ParseRetentionDef("10s:14d") Retention{10, 120960}

  See: http://graphite.readthedocs.org/en/1.0/config-carbon.html#storage-schemas-conf
*/
func ParseRetentionDef(retentionDef string) (*Retention, error) {
	parts := strings.Split(retentionDef, ":")
	if len(parts) != 2 {
		return nil, fmt.Errorf("Not enough parts in retentionDef [%v]", retentionDef)
	}
	precision, err := parseRetentionPart(parts[0])
	if err != nil {
		return nil, fmt.Errorf("Failed to parse precision: %v", err)
	}

	points, err := parseRetentionPart(parts[1])
	if err != nil {
		return nil, fmt.Errorf("Failed to parse points: %v", err)
	}
	points /= precision

	return &Retention{precision, points}, err
}

func ParseRetentionDefs(retentionDefs string) (Retentions, error) {
	retentions := make(Retentions, 0)
	for _, retentionDef := range strings.Split(retentionDefs, ",") {
		retention, err := ParseRetentionDef(retentionDef)
		if err != nil {
			return nil, err
		}
		retentions = append(retentions, retention)
	}
	return retentions, nil
}

/*
	Represents a Whisper database file.
*/
type Whisper struct {
	file *os.File

	// Metadata
	aggregationMethod AggregationMethod
	maxRetention      int
	xFilesFactor      float32
	archives          []archiveInfo
}

/*
	Create a new Whisper database file and write it's header.
*/
func Create(path string, retentions Retentions, aggregationMethod AggregationMethod, xFilesFactor float32) (whisper *Whisper, err error) {
	sort.Sort(RetentionsByPrecision{retentions})
	if err = validateRetentions(retentions); err != nil {
		return nil, err
	}
	_, err = os.Stat(path)
	if err == nil {
		return nil, os.ErrExist
	}
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	// Lock file as carbon-cache.py would
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		file.Close()
		return nil, err
	}
	whisper = new(Whisper)

	// Set the metadata
	whisper.file = file
	whisper.aggregationMethod = aggregationMethod
	whisper.xFilesFactor = xFilesFactor
	for _, retention := range retentions {
		if retention.MaxRetention() > whisper.maxRetention {
			whisper.maxRetention = retention.MaxRetention()
		}
	}

	// Set the archive info
	offset := MetadataSize + (ArchiveInfoSize * len(retentions))
	whisper.archives = make([]archiveInfo, 0, len(retentions))
	for _, retention := range retentions {
		whisper.archives = append(whisper.archives, archiveInfo{*retention, offset})
		offset += retention.Size()
	}

	err = whisper.writeHeader()
	if err != nil {
		return nil, err
	}

	// pre-allocate file size, fallocate proved slower
	remaining := whisper.Size() - whisper.MetadataSize()
	chunkSize := 16384
	zeros := make([]byte, chunkSize)
	for remaining > chunkSize {
		whisper.file.Write(zeros)
		remaining -= chunkSize
	}
	whisper.file.Write(zeros[:remaining])
	whisper.file.Sync()

	return whisper, nil
}

func validateRetentions(retentions Retentions) error {
	if len(retentions) == 0 {
		return fmt.Errorf("No retentions")
	}
	for i, retention := range retentions {
		if i == len(retentions)-1 {
			break
		}

		nextRetention := retentions[i+1]
		if !(retention.secondsPerPoint < nextRetention.secondsPerPoint) {
			return fmt.Errorf("A Whisper database may not be configured having two archives with the same precision (archive%v: %v, archive%v: %v)", i, retention, i+1, nextRetention)
		}

		if mod(nextRetention.secondsPerPoint, retention.secondsPerPoint) != 0 {
			return fmt.Errorf("Higher precision archives' precision must evenly divide all lower precision archives' precision (archive%v: %v, archive%v: %v)", i, retention.secondsPerPoint, i+1, nextRetention.secondsPerPoint)
		}

		if retention.MaxRetention() >= nextRetention.MaxRetention() {
			return fmt.Errorf("Lower precision archives must cover larger time intervals than higher precision archives (archive%v: %v seconds, archive%v: %v seconds)", i, retention.MaxRetention(), i+1, nextRetention.MaxRetention())
		}

		if retention.numberOfPoints < (nextRetention.secondsPerPoint / retention.secondsPerPoint) {
			return fmt.Errorf("Each archive must have at least enough points to consolidate to the next archive (archive%v consolidates %v of archive%v's points but it has only %v total points)", i+1, nextRetention.secondsPerPoint/retention.secondsPerPoint, i, retention.numberOfPoints)
		}
	}
	return nil
}

/*
  Open an existing Whisper database and read it's header
*/
func Open(path string) (whisper *Whisper, err error) {
	file, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	// Lock file as carbon-cache.py would
	if err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		file.Close()
		return nil, err
	}

	whisper = new(Whisper)
	whisper.file = file

	// read the metadata
	b := make([]byte, MetadataSize)
	offset := 0
	file.Read(b)
	whisper.aggregationMethod = AggregationMethod(unpackInt(b[offset : offset+IntSize]))
	offset += IntSize
	whisper.maxRetention = unpackInt(b[offset : offset+IntSize])
	offset += IntSize
	whisper.xFilesFactor = unpackFloat32(b[offset : offset+FloatSize])
	offset += FloatSize
	archiveCount := unpackInt(b[offset : offset+IntSize])
	offset += IntSize

	// read the archive info
	b = make([]byte, ArchiveInfoSize*archiveCount)
	file.Read(b)
	whisper.archives = make([]archiveInfo, archiveCount)
	for i := 0; i < archiveCount; i++ {
		whisper.archives[i] = unpackArchiveInfo(b[i*ArchiveInfoSize : (i+1)*ArchiveInfoSize])
	}

	return whisper, nil
}

func (whisper *Whisper) writeHeader() (err error) {
	b := make([]byte, whisper.MetadataSize())
	i := 0
	i += packInt(b, int(whisper.aggregationMethod), i)
	i += packInt(b, whisper.maxRetention, i)
	i += packFloat32(b, whisper.xFilesFactor, i)
	i += packInt(b, len(whisper.archives), i)
	for _, archive := range whisper.archives {
		i += packInt(b, archive.offset, i)
		i += packInt(b, archive.secondsPerPoint, i)
		i += packInt(b, archive.numberOfPoints, i)
	}
	_, err = whisper.file.Write(b)

	return err
}

/*
  Close the whisper file
*/
func (whisper *Whisper) Close() {
	// This releases any held Flock style locks
	whisper.file.Close()
}

/*
  Calculate the total number of bytes the Whisper file should be according to the metadata.
*/
func (whisper *Whisper) Size() int {
	size := whisper.MetadataSize()
	for _, archive := range whisper.archives {
		size += archive.Size()
	}
	return size
}

/*
  Calculate the number of bytes the metadata section will be.
*/
func (whisper *Whisper) MetadataSize() int {
	return MetadataSize + (ArchiveInfoSize * len(whisper.archives))
}

/*
  Retentions returns a Retentions type which lists each archive's retention
  in this Whisper file.  A deep copy so that the internal data structure
  remains safe.
*/
func (whisper *Whisper) Retentions() (retentions Retentions) {
	retentions = make(Retentions, 0)
	for _, v := range whisper.archives {
		r := new(Retention)
		r.secondsPerPoint = v.Retention.secondsPerPoint
		r.numberOfPoints = v.Retention.numberOfPoints
		retentions = append(retentions, r)
	}

	return retentions
}

/*
  Update a value in the database.

  If the timestamp is in the future or outside of the maximum retention it will
  fail immediately.
*/
func (whisper *Whisper) Update(value float64, timestamp int) (err error) {
	diff := int(time.Now().Unix()) - timestamp
	if !(diff < whisper.maxRetention && diff >= 0) {
		return fmt.Errorf("Timestamp not covered by any archives in this database")
	}
	var archive archiveInfo
	var lowerArchives []archiveInfo
	var i int
	for i, archive = range whisper.archives {
		if archive.MaxRetention() < diff {
			continue
		}
		lowerArchives = whisper.archives[i+1:] // TODO: investigate just returning the positions
		break
	}

	myInterval := timestamp - mod(timestamp, archive.secondsPerPoint)
	point := dataPoint{myInterval, value}

	_, err = whisper.file.WriteAt(point.Bytes(), whisper.getPointOffset(myInterval, &archive))
	if err != nil {
		return err
	}

	higher := archive
	for _, lower := range lowerArchives {
		propagated, err := whisper.propagate(myInterval, &higher, &lower)
		if err != nil {
			return err
		} else if !propagated {
			break
		}
		higher = lower
	}

	return nil
}

// UpdateManyWithRetention updates only archive with specified retention.
// retention = -1 means "update all possible archives"
func (whisper *Whisper) UpdateManyWithRetention(points []*TimeSeriesPoint, retention int) {
	// sort the points, newest first
	sort.Sort(timeSeriesPointsNewestFirst{points})

	now := int(time.Now().Unix()) // TODO: danger of 2030 something overflow

	var currentPoints []*TimeSeriesPoint
	for _, archive := range whisper.archives {
		if retention != -1 && retention != archive.MaxRetention() {
			continue
		}
		currentPoints, points = extractPoints(points, now, archive.MaxRetention())
		if len(currentPoints) == 0 {
			continue
		}
		// reverse currentPoints
		for i, j := 0, len(currentPoints)-1; i < j; i, j = i+1, j-1 {
			currentPoints[i], currentPoints[j] = currentPoints[j], currentPoints[i]
		}
		whisper.archiveUpdateMany(&archive, currentPoints)

		if len(points) == 0 { // nothing left to do
			break
		}
	}
}

func (whisper *Whisper) UpdateMany(points []*TimeSeriesPoint) {
	whisper.UpdateManyWithRetention(points, -1)
}

func (whisper *Whisper) archiveUpdateMany(archive *archiveInfo, points []*TimeSeriesPoint) {
	alignedPoints := alignPoints(archive, points)
	intervals, packedBlocks := packSequences(archive, alignedPoints)

	baseInterval := whisper.getBaseInterval(archive)
	if baseInterval == 0 {
		baseInterval = intervals[0]
	}

	for i := range intervals {
		myOffset := archive.PointOffset(baseInterval, intervals[i])
		bytesBeyond := int(myOffset-archive.End()) + len(packedBlocks[i])
		if bytesBeyond > 0 {
			pos := len(packedBlocks[i]) - bytesBeyond
			whisper.file.WriteAt(packedBlocks[i][:pos], myOffset)
			whisper.file.WriteAt(packedBlocks[i][pos:], archive.Offset())
		} else {
			whisper.file.WriteAt(packedBlocks[i], myOffset)
		}
	}

	higher := *archive
	lowerArchives := whisper.lowerArchives(archive)

	for _, lower := range lowerArchives {
		seen := make(map[int]bool)
		propagateFurther := false
		for _, point := range alignedPoints {
			interval := point.interval - mod(point.interval, lower.secondsPerPoint)
			if !seen[interval] {
				if propagated, err := whisper.propagate(interval, &higher, &lower); err != nil {
					panic("Failed to propagate")
				} else if propagated {
					propagateFurther = true
				}
			}
		}
		if !propagateFurther {
			break
		}
		higher = lower
	}
}

func extractPoints(points []*TimeSeriesPoint, now int, maxRetention int) (currentPoints []*TimeSeriesPoint, remainingPoints []*TimeSeriesPoint) {
	maxAge := now - maxRetention
	for i, point := range points {
		if point.Time < maxAge {
			if i > 0 {
				return points[:i-1], points[i-1:]
			} else {
				return []*TimeSeriesPoint{}, points
			}
		}
	}
	return points, remainingPoints
}

func alignPoints(archive *archiveInfo, points []*TimeSeriesPoint) []dataPoint {
	alignedPoints := make([]dataPoint, 0, len(points))
	positions := make(map[int]int)
	for _, point := range points {
		dPoint := dataPoint{point.Time - mod(point.Time, archive.secondsPerPoint), point.Value}
		if p, ok := positions[dPoint.interval]; ok {
			alignedPoints[p] = dPoint
		} else {
			alignedPoints = append(alignedPoints, dPoint)
			positions[dPoint.interval] = len(alignedPoints) - 1
		}
	}
	return alignedPoints
}

func packSequences(archive *archiveInfo, points []dataPoint) (intervals []int, packedBlocks [][]byte) {
	intervals = make([]int, 0)
	packedBlocks = make([][]byte, 0)
	for i, point := range points {
		if i == 0 || point.interval != intervals[len(intervals)-1]+archive.secondsPerPoint {
			intervals = append(intervals, point.interval)
			packedBlocks = append(packedBlocks, point.Bytes())
		} else {
			packedBlocks[len(packedBlocks)-1] = append(packedBlocks[len(packedBlocks)-1], point.Bytes()...)
		}
	}
	return
}

/*
	Calculate the offset for a given interval in an archive

	This method retrieves the baseInterval and the
*/
func (whisper *Whisper) getPointOffset(start int, archive *archiveInfo) int64 {
	baseInterval := whisper.getBaseInterval(archive)
	if baseInterval == 0 {
		return archive.Offset()
	}
	return archive.PointOffset(baseInterval, start)
}

func (whisper *Whisper) getBaseInterval(archive *archiveInfo) int {
	baseInterval, err := whisper.readInt(archive.Offset())
	if err != nil {
		panic("Failed to read baseInterval")
	}
	return baseInterval
}

func (whisper *Whisper) lowerArchives(archive *archiveInfo) (lowerArchives []archiveInfo) {
	for i, lower := range whisper.archives {
		if lower.secondsPerPoint > archive.secondsPerPoint {
			return whisper.archives[i:]
		}
	}
	return
}

func (whisper *Whisper) propagate(timestamp int, higher, lower *archiveInfo) (bool, error) {
	lowerIntervalStart := timestamp - mod(timestamp, lower.secondsPerPoint)

	higherFirstOffset := whisper.getPointOffset(lowerIntervalStart, higher)

	// TODO: extract all this series extraction stuff
	higherPoints := lower.secondsPerPoint / higher.secondsPerPoint
	higherSize := higherPoints * PointSize
	relativeFirstOffset := higherFirstOffset - higher.Offset()
	relativeLastOffset := int64(mod(int(relativeFirstOffset+int64(higherSize)), higher.Size()))
	higherLastOffset := relativeLastOffset + higher.Offset()

	series := whisper.readSeries(higherFirstOffset, higherLastOffset, higher)

	// and finally we construct a list of values
	knownValues := make([]float64, 0, len(series))
	currentInterval := lowerIntervalStart

	for _, dPoint := range series {
		if dPoint.interval == currentInterval {
			knownValues = append(knownValues, dPoint.value)
		}
		currentInterval += higher.secondsPerPoint
	}

	// propagate aggregateValue to propagate from neighborValues if we have enough known points
	if len(knownValues) == 0 {
		return false, nil
	}
	knownPercent := float32(len(knownValues)) / float32(len(series))
	if knownPercent < whisper.xFilesFactor { // check we have enough data points to propagate a value
		return false, nil
	} else {
		aggregateValue := aggregate(whisper.aggregationMethod, knownValues)
		point := dataPoint{lowerIntervalStart, aggregateValue}
		whisper.file.WriteAt(point.Bytes(), whisper.getPointOffset(lowerIntervalStart, lower))
	}
	return true, nil
}

// TODO: add error handling
func (whisper *Whisper) readSeries(start, end int64, archive *archiveInfo) []dataPoint {
	var b []byte
	if start < end {
		b = make([]byte, end-start)
		whisper.file.ReadAt(b, start)
	} else {
		b = make([]byte, archive.End()-start)
		whisper.file.ReadAt(b, start)
		b2 := make([]byte, end-archive.Offset())
		whisper.file.ReadAt(b2, archive.Offset())
		b = append(b, b2...)
	}
	return unpackDataPoints(b)
}

/*
  Calculate the starting time for a whisper db.
*/
func (whisper *Whisper) StartTime() int {
	now := int(time.Now().Unix()) // TODO: danger of 2030 something overflow
	return now - whisper.maxRetention
}

/*
  Fetch a TimeSeries for a given time span from the file.
*/
func (whisper *Whisper) Fetch(fromTime, untilTime int) (timeSeries *TimeSeries, err error) {
	now := int(time.Now().Unix()) // TODO: danger of 2030 something overflow
	if fromTime > untilTime {
		return nil, fmt.Errorf("Invalid time interval: from time '%d' is after until time '%d'", fromTime, untilTime)
	}
	oldestTime := whisper.StartTime()
	// range is in the future
	if fromTime > now {
		return nil, nil
	}
	// range is beyond retention
	if untilTime < oldestTime {
		return nil, nil
	}
	if fromTime < oldestTime {
		fromTime = oldestTime
	}
	if untilTime > now {
		untilTime = now
	}

	// TODO: improve this algorithm it's ugly
	diff := now - fromTime
	var archive archiveInfo
	for _, archive = range whisper.archives {
		if archive.MaxRetention() >= diff {
			break
		}
	}

	fromInterval := archive.Interval(fromTime)
	untilInterval := archive.Interval(untilTime)
	baseInterval := whisper.getBaseInterval(&archive)

	if baseInterval == 0 {
		step := archive.secondsPerPoint
		points := (untilInterval - fromInterval) / step
		values := make([]float64, points)
		for i, _ := range values {
			values[i] = math.NaN()
		}
		return &TimeSeries{fromInterval, untilInterval, step, values}, nil
	}

	fromOffset := archive.PointOffset(baseInterval, fromInterval)
	untilOffset := archive.PointOffset(baseInterval, untilInterval)

	series := whisper.readSeries(fromOffset, untilOffset, &archive)

	values := make([]float64, len(series))
	for i, _ := range values {
		values[i] = math.NaN()
	}
	currentInterval := fromInterval
	step := archive.secondsPerPoint

	for i, dPoint := range series {
		if dPoint.interval == currentInterval {
			values[i] = dPoint.value
		}
		currentInterval += step
	}

	return &TimeSeries{fromInterval, untilInterval, step, values}, nil
}

func (whisper *Whisper) readInt(offset int64) (int, error) {
	// TODO: make errors better
	b := make([]byte, IntSize)
	_, err := whisper.file.ReadAt(b, offset)
	if err != nil {
		return 0, err
	}

	return unpackInt(b), nil
}

/*
  A retention level.

  Retention levels describe a given archive in the database. How detailed it is and how far back
  it records.
*/
type Retention struct {
	secondsPerPoint int
	numberOfPoints  int
}

func (retention *Retention) MaxRetention() int {
	return retention.secondsPerPoint * retention.numberOfPoints
}

func (retention *Retention) Size() int {
	return retention.numberOfPoints * PointSize
}

func (retention *Retention) SecondsPerPoint() int {
	return retention.secondsPerPoint
}

func (retention *Retention) NumberOfPoints() int {
	return retention.numberOfPoints
}

type Retentions []*Retention

func (r Retentions) Len() int {
	return len(r)
}

func (r Retentions) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

type RetentionsByPrecision struct{ Retentions }

func (r RetentionsByPrecision) Less(i, j int) bool {
	return r.Retentions[i].secondsPerPoint < r.Retentions[j].secondsPerPoint
}

func (r RetentionsByPrecision) Iterator() Retentions {
	return r.Retentions
}

/*
  Describes a time series in a file.

  The only addition this type has over a Retention is the offset at which it exists within the
  whisper file.
*/
type archiveInfo struct {
	Retention
	offset int
}

func (archive *archiveInfo) Offset() int64 {
	return int64(archive.offset)
}

func (archive *archiveInfo) PointOffset(baseInterval, interval int) int64 {
	timeDistance := interval - baseInterval
	pointDistance := timeDistance / archive.secondsPerPoint
	byteDistance := pointDistance * PointSize
	myOffset := archive.Offset() + int64(mod(byteDistance, archive.Size()))

	return myOffset
}

func (archive *archiveInfo) End() int64 {
	return archive.Offset() + int64(archive.Size())
}

func (archive *archiveInfo) Interval(time int) int {
	return time - mod(time, archive.secondsPerPoint) + archive.secondsPerPoint
}

type TimeSeries struct {
	fromTime  int
	untilTime int
	step      int
	values    []float64
}

func (ts *TimeSeries) FromTime() int {
	return ts.fromTime
}

func (ts *TimeSeries) UntilTime() int {
	return ts.untilTime
}

func (ts *TimeSeries) Step() int {
	return ts.step
}

func (ts *TimeSeries) Values() []float64 {
	return ts.values
}

func (ts *TimeSeries) Points() []*TimeSeriesPoint {
	points := make([]*TimeSeriesPoint, len(ts.values))
	for i, value := range ts.values {
		points[i] = &TimeSeriesPoint{Time: ts.fromTime + ts.step*i, Value: value}
	}
	return points
}

func (ts *TimeSeries) String() string {
	return fmt.Sprintf("TimeSeries{'%v' '%-v' %v %v}", time.Unix(int64(ts.fromTime), 0), time.Unix(int64(ts.untilTime), 0), ts.step, ts.values)
}

type TimeSeriesPoint struct {
	Time  int
	Value float64
}

type timeSeriesPoints []*TimeSeriesPoint

func (p timeSeriesPoints) Len() int {
	return len(p)
}

func (p timeSeriesPoints) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type timeSeriesPointsNewestFirst struct {
	timeSeriesPoints
}

func (p timeSeriesPointsNewestFirst) Less(i, j int) bool {
	return p.timeSeriesPoints[i].Time > p.timeSeriesPoints[j].Time
}

type dataPoint struct {
	interval int
	value    float64
}

func (point *dataPoint) Bytes() []byte {
	b := make([]byte, PointSize)
	packInt(b, point.interval, 0)
	packFloat64(b, point.value, IntSize)
	return b
}

func sum(values []float64) float64 {
	result := 0.0
	for _, value := range values {
		result += value
	}
	return result
}

func aggregate(method AggregationMethod, knownValues []float64) float64 {
	switch method {
	case Average:
		return sum(knownValues) / float64(len(knownValues))
	case Sum:
		return sum(knownValues)
	case Last:
		return knownValues[len(knownValues)-1]
	case Max:
		max := knownValues[0]
		for _, value := range knownValues {
			if value > max {
				max = value
			}
		}
		return max
	case Min:
		min := knownValues[0]
		for _, value := range knownValues {
			if value < min {
				min = value
			}
		}
		return min
	}
	panic("Invalid aggregation method")
}

func packInt(b []byte, v, i int) int {
	binary.BigEndian.PutUint32(b[i:i+IntSize], uint32(v))
	return IntSize
}

func packFloat32(b []byte, v float32, i int) int {
	binary.BigEndian.PutUint32(b[i:i+FloatSize], math.Float32bits(v))
	return FloatSize
}

func packFloat64(b []byte, v float64, i int) int {
	binary.BigEndian.PutUint64(b[i:i+Float64Size], math.Float64bits(v))
	return Float64Size
}

func unpackInt(b []byte) int {
	return int(binary.BigEndian.Uint32(b))
}

func unpackFloat32(b []byte) float32 {
	return math.Float32frombits(binary.BigEndian.Uint32(b))
}

func unpackFloat64(b []byte) float64 {
	return math.Float64frombits(binary.BigEndian.Uint64(b))
}

func unpackArchiveInfo(b []byte) archiveInfo {
	return archiveInfo{Retention{unpackInt(b[IntSize : IntSize*2]), unpackInt(b[IntSize*2 : IntSize*3])}, unpackInt(b[:IntSize])}
}

func unpackDataPoint(b []byte) dataPoint {
	return dataPoint{unpackInt(b[0:IntSize]), unpackFloat64(b[IntSize:PointSize])}
}

func unpackDataPoints(b []byte) (series []dataPoint) {
	series = make([]dataPoint, 0, len(b)/PointSize)
	for i := 0; i < len(b); i += PointSize {
		series = append(series, unpackDataPoint(b[i:i+PointSize]))
	}
	return
}

/*
	Implementation of modulo that works like Python
	Thanks @timmow for this
*/
func mod(a, b int) int {
	return a - (b * int(math.Floor(float64(a)/float64(b))))
}
