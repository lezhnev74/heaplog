package db

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"heaplog_2024/common"
)

type SegmentsDb struct {
	db *sql.DB
}

type Segment struct {
	Id               int
	FileId           int
	Loc              common.Location
	DateMin, DateMax time.Time
}

func NewSegmentsDb(db *sql.DB) *SegmentsDb {
	return &SegmentsDb{db: db}
}

func (sdb *SegmentsDb) ReadIndexedLocations(fileId int) ([]common.Location, error) {
	selectSql := `
	SELECT posFrom, posTo 
	FROM file_segments
	WHERE fileId=?
	ORDER BY posFrom ASC
`
	r, err := sdb.db.Query(selectSql, fileId)
	if err != nil {
		return nil, fmt.Errorf("unable to read existing file segments: %w", err)
	}
	defer func() { _ = r.Close() }()

	indexedLocations := make([]common.Location, 0, 20)
	for r.Next() {
		is := common.Location{}
		err = r.Scan(&is.From, &is.To)
		if err != nil {
			return nil, fmt.Errorf("unable to read existing file segments: %w", err)
		}
		indexedLocations = append(indexedLocations, is)
	}
	return indexedLocations, nil
}

func (sdb *SegmentsDb) CheckinSegment(
	fileId int,
	loc common.Location,
	min, max time.Time,
) (segmentId uint32, err error) {
	segmentId, err = sdb.ReserveSegmentId()
	if err != nil {
		err = fmt.Errorf("unable to check in segments (1): %w", err)
		return
	}

	err = sdb.CheckinSegmentWithId(segmentId, fileId, loc, min, max)
	return
}

func (sdb *SegmentsDb) CheckinSegmentWithId(
	segmentId uint32,
	fileId int,
	loc common.Location,
	min, max time.Time,
) (err error) {

	r := sdb.db.QueryRow("SELECT Id FROM file_segments WHERE Id=?", segmentId)

	var x int
	err = r.Scan(&x)

	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		insertSQL := `
	INSERT INTO file_segments(Id, fileId, posFrom, posTo, DateMin, DateMax) 
	VALUES (?,?,?,?,?,?)
`
		_, err = sdb.db.Exec(
			insertSQL,
			segmentId,
			uint(fileId),
			loc.From,
			loc.To,
			uint64(min.UnixMicro()),
			uint64(max.UnixMicro()),
		)
		if err != nil {
			err = fmt.Errorf("unable to check in segments with id (1): %w", err)
		}
		return
	}

	updateSql := `
	UPDATE file_segments 
	SET posFrom=?, posTo=?, DateMin=?, DateMax=?
	WHERE Id=?
`
	_, err = sdb.db.Exec(
		updateSql,
		loc.From,
		loc.To,
		min.UnixMicro(),
		max.UnixMicro(),
		segmentId,
	)
	if err != nil {
		err = fmt.Errorf("unable to check in segments with id: %w", err)
	}
	return
}

func (sdb *SegmentsDb) ReserveSegmentId() (segmentId uint32, err error) {
	r := sdb.db.QueryRow(`SELECT nextval('segment_ids');`)
	err = r.Scan(&segmentId)
	if err != nil {
		err = fmt.Errorf("unable to check in a segment: %w", err)
	}
	return
}
func (sdb *SegmentsDb) LastSegmentLocation(fileId int) (location common.Location, err error) {
	r := sdb.db.QueryRow(`SELECT posFrom,posTo FROM file_segments WHERE fileId=? ORDER BY posTo DESC LIMIT 1`, fileId)
	err = r.Scan(&location.From, &location.To)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil // return empty location
		return
	} else if err != nil {
		err = fmt.Errorf("unable to read the last segment: %w", err)
	}
	return
}

func (sdb *SegmentsDb) AllSegmentIds(min, max *time.Time) ([]uint32, error) {
	q := `
		SELECT Id FROM file_segments
	  	WHERE DateMin >= ? AND DateMax <= ?
	  	ORDER BY dateMin ASC -- important sort by date, used in search to go from earliest to latest Messages
`
	minDate := int64(0)
	if min != nil {
		minDate = min.UnixMicro()
	}
	maxDate := int64(math.MaxInt64)
	if max != nil {
		maxDate = max.UnixMicro()
	}

	r, err := sdb.db.Query(q, minDate, maxDate)
	if err != nil {
		return nil, fmt.Errorf("unable to read existing file segments: %w", err)
	}
	defer func() { _ = r.Close() }()

	segments := make([]uint32, 0)
	for r.Next() {
		id := uint32(0)
		err = r.Scan(&id)
		if err != nil {
			return nil, fmt.Errorf("unable to read segments: %w", err)
		}
		segments = append(segments, id)
	}
	return segments, nil
}

// SelectSegmentThatAdjoins find existing half-full segment where a new message can be appended to.
func (sdb *SegmentsDb) SelectSegmentThatAdjoins(fileId int, messagePos uint64) (s Segment, err error) {
	r := sdb.db.QueryRow("SELECT * FROM file_segments WHERE posTo=?", messagePos)
	var dateMin, dateMax int64
	err = r.Scan(&s.Id, &s.FileId, &s.Loc.From, &s.Loc.To, &dateMin, &dateMax)
	s.DateMin = time.UnixMicro(dateMin)
	s.DateMax = time.UnixMicro(dateMax)
	return
}

func (sdb *SegmentsDb) FilterByDates(segments []uint32, min, max *time.Time) ([]uint32, error) {

	if len(segments) == 0 {
		return segments, nil
	}

	q := `
SELECT Id FROM file_segments
          WHERE Id IN (%s) AND DateMin >= ? AND DateMax <= ?
          ORDER BY dateMin ASC -- important sort by date, used in search to go from earliest to latest Messages
`
	q = fmt.Sprintf(q, strings.TrimRight(strings.Repeat("?,", len(segments)), ","))
	queryArgs := common.SliceToAny(segments)

	minDate := int64(0)
	if min != nil {
		minDate = min.UnixMicro()
	}
	maxDate := int64(math.MaxInt64)
	if max != nil {
		maxDate = max.UnixMicro()
	}
	queryArgs = append(queryArgs, minDate, maxDate)

	r, err := sdb.db.Query(q, queryArgs...)
	if err != nil {
		return nil, fmt.Errorf("unable to read existing file segments: %w", err)
	}
	defer func() { _ = r.Close() }()

	filteredSegments := make([]uint32, 0)
	for r.Next() {
		id := uint32(0)
		err = r.Scan(&id)
		if err != nil {
			return nil, fmt.Errorf("unable to read segments: %w", err)
		}
		filteredSegments = append(filteredSegments, id)
	}
	return filteredSegments, nil
}
