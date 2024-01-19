package storage

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"github.com/marcboeker/go-duckdb"
	"golang.org/x/exp/maps"
	"golang.org/x/xerrors"
	"hash/crc32"
	"heaplog/common"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"
)

// Storage is a low-level module that consistently stores data on disk.
// It has data-oriented(CRUD) API.
// It manages both duckdb instance and inverted index (II).

//go:embed migrations
var migrationFS embed.FS

var ErrNoData error = xerrors.Errorf("no data available")
var crc32c = crc32.MakeTable(crc32.Castagnoli)

// memoryReportCh accepts request for printing db stats (the argument controls the max pace of prints)
var memoryReportCh chan time.Duration

type Storage struct {
	db *sql.DB // duckdb connection

	// incomingQueryMessages come from the ingestion flow
	incomingQueryMessages chan common.MatchedMessage
	// queryMessageAppender accepts matched messages from the search flow
	queryMessageAppender *duckdb.Appender

	// this lock synchronises access to "files" table while check-ins
	filesLock sync.Mutex

	// Checking In Segments:
	termsDir *TermsDir

	incomingSegmentTermsLastFlush     time.Time
	incomingSegmentTermsLastFlushLock sync.RWMutex

	incomingSegmentTerms chan appendSegmentTerm // row values for the appender
	segmentTermsAppender *duckdb.Appender

	incomingSegmentMessageLastFlush time.Time
	incomingSegmentMessage          chan appendSegmentMessage // row values for the appender
	segmentMessagesAppender         *duckdb.Appender
	segmentMessagesTailsAppender    *duckdb.Appender

	appendersFlushInterval time.Duration
}

type appendSegmentMessage struct {
	messageId, segmentId, locMin, locMax, dateUnixMicro int64
	isTail                                              bool
}
type appendSegmentTerm struct {
	tid uint32
	sid uint32
}

func (s *Storage) GetDb() *sql.DB { return s.db }

/*
CheckInFiles replaces checked in files with the given.
It returns obsolete files so the client can do other clean-ups like inverted index removal.
*/
func (s *Storage) CheckInFiles(files map[string]int64) (obsoleteFiles []string, newFiles []string, err error) {
	// concurrent writes for the same row in a table will fail,
	// so we need to synchronize writing access to this table

	s.filesLock.Lock()
	defer s.filesLock.Unlock()

	// 1. Populate new files
	curFiles, err := s.AllFiles()
	if err != nil {
		return
	}
	curFilesPaths := maps.Keys(curFiles)
	for f := range files {
		if !slices.Contains(curFilesPaths, f) {
			newFiles = append(newFiles, f)
		}
	}

	filePaths := maps.Keys(files)
	insertSQL := fmt.Sprintf(
		"INSERT OR REPLACE INTO files(path,fileHash,size) VALUES %s",
		strings.TrimRight(strings.Repeat("(?,?,?),", len(files)), ","),
	)

	insertBindings := make([]any, 0, 3*len(files))
	for p, siz := range files {
		insertBindings = append(insertBindings, p, common.HashFile(p), siz)
	}

	if len(filePaths) > 0 {
		_, err = s.db.Exec(insertSQL, insertBindings...)
		if err != nil {
			return
		}
	}

	// 2. Remove obsolete files
	path := ""
	existingFilesWhereExpr := fmt.Sprintf("(%s)", strings.TrimRight(strings.Repeat("?,", len(files)), ","))

	if len(files) == 0 {
		existingFilesWhereExpr = "('')" // no-op
	}

	r, err := s.db.Query(
		fmt.Sprintf("SELECT path FROM files WHERE path NOT IN %s", existingFilesWhereExpr),
		common.SliceToAny(filePaths)...,
	)
	if err != nil {
		return
	}
	defer r.Close()

	for r.Next() {
		err = r.Scan(&path)
		if err != nil {
			return
		}

		obsoleteFiles = append(obsoleteFiles, path)
	}

	// 2.1 Remove files entry
	_, err = s.db.Exec(
		fmt.Sprintf("DELETE FROM files WHERE path NOT IN %s", existingFilesWhereExpr),
		common.SliceToAny(filePaths)...,
	)
	if err != nil {
		log.Printf("unable to delete obsolete files: %s", err)
		return
	}

	// 2.2 Remove segments
	for _, obsoleteFile := range obsoleteFiles {
		_, err = s.db.Exec(
			`DELETE FROM file_segments WHERE fileHash = ?`,
			common.HashFile(obsoleteFile),
		)
		if err != nil {
			log.Printf("unable to delete obsolete segments: %s", err)
			return
		}
	}

	// 2.3 Cleanups
	// Cleanups for interrupted segment checkins (see CheckInSegment):
	_, err = s.db.Exec(
		`delete from file_segments_terms where segment_id NOT IN (select id from file_segments);`,
	)
	if err != nil {
		log.Printf("unable to delete interrupted segment terms: %s", err)
		return
	}

	_, err = s.db.Exec(
		`delete from file_segments_messages where segment_id NOT IN (select id from file_segments);`,
	)
	if err != nil {
		log.Printf("unable to delete interrupted segment messages: %s", err)
		return
	}

	// Remove from query results
	_, err = s.db.Exec(
		`delete from query_results where message_id NOT IN (select id from file_segments_messages);`,
	)
	if err != nil {
		log.Printf("unable to delete query messages: %s", err)
		return
	}

	return
}

func (s *Storage) AllFiles() (files map[string]int64, err error) {

	var (
		path string
		size int64
	)
	files = make(map[string]int64)

	selectResults, err := s.db.Query("SELECT path,size FROM files")
	if err != nil {
		return nil, err
	}
	defer selectResults.Close()

	for selectResults.Next() {
		err = selectResults.Scan(&path, &size)
		if err != nil {
			return nil, err
		}
		files[path] = size
	}

	return
}

func (s *Storage) ReadFileByHash(ds common.DataSourceHash) (path string, err error) {
	r := s.db.QueryRow("SELECT path FROM files WHERE fileHash=?", ds)
	if r.Err() != nil {
		return "", r.Err()
	}

	err = r.Scan(&path)
	return
}

// UpdateTailMessage will update the tail segment's last message with fresh indexed data.
// That is to address "tail fragmented messages" problem.
func (s *Storage) UpdateTailMessage(messageId int, newLoc common.Location, terms []string) error {
	var (
		segmentId, segmentPosTo int64
		tid                     uint32
	)

	r := s.db.QueryRow(
		`
		SELECT file_segments_messages.segment_id, file_segments.posTo
		FROM file_segments_messages 
		JOIN file_segments ON file_segments.id=file_segments_messages.segment_id
		WHERE file_segments_messages.id=?
`,
		messageId,
	)
	err := r.Scan(&segmentId, &segmentPosTo)
	if err != nil {
		return err
	}

	// 1. Update message location
	_, err = s.db.Exec("UPDATE file_segments_messages SET posTo=? WHERE id=?", newLoc.Max, messageId)
	if err != nil {
		return err
	}

	if segmentPosTo < newLoc.Max {
		_, err = s.db.Exec("UPDATE file_segments SET posTo=? WHERE id=?", newLoc.Max, segmentId)
		if err != nil {
			return err
		}
	}

	// 2. Checkin terms
	messageTids, err := s.checkInTerms(terms)
	if err != nil {
		return err
	}

	// 3. Add missing terms for the segment
	segmentTids := make([]uint32, 0)
	rr, err := s.db.Query("SELECT term_id FROM file_segments_terms WHERE segment_id=?", segmentId)
	if err != nil {
		return err
	}
	defer rr.Close()
	for rr.Next() {
		err = rr.Scan(&tid)
		if err != nil {
			return err
		}
		segmentTids = append(segmentTids, tid)
	}

	slices.Sort(messageTids)
	slices.Sort(segmentTids)

	// filter message tids in place (remove tids that are already in the system)
	x := 0
	for _, messageTid := range messageTids {
		if _, ok := slices.BinarySearch(segmentTids, messageTid); ok {
			continue
		}
		messageTids[x] = messageTid
		x++
	}
	messageTids = messageTids[:x]

	for _, messageTid := range messageTids {
		s.incomingSegmentTerms <- appendSegmentTerm{messageTid, uint32(segmentId)}
	}

	return nil
}

// CheckInSegment saves a segment to the duckdb and to II.
// it is a 2-phase commit. It returns the segment id.
func (s *Storage) CheckInSegment(segment common.IndexedSegment, terms []string) (int, error) {
	// For performance consideration, here is how it works:
	// it reserves a segment id (increases no matter if tx failed or not) and inserts that to file_segments in a Tx.
	// if anything fails it drops tx and the segment is not considered as indexed.
	// however all other writes are done outside of tx via appenders.
	// It means that it is not protected by the Tx boundaries.
	// As a result, failed txs leave possibly big writes in the db that has no value,
	// there must be GC for those, like "delete * from file_segments_messages where segment_id... is not found in file_segments"

	// first phase: reserve Segment ID in a Tx
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	var segmentId int
	r := tx.QueryRow(`SELECT nextval('segment_ids');`)
	err = r.Scan(&segmentId)
	if err != nil {
		return 0, xerrors.Errorf("unable to check in segments: %w", err)
	}

	insertSQL := `
	INSERT INTO file_segments(id, fileHash, posFrom, posTo, dateMin, dateMax) 
	VALUES (?,?,?,?,?,?)
`
	_, err = tx.Exec(
		insertSQL,
		segmentId,
		segment.DataSource,
		segment.Loc().Min,
		segment.Loc().Max,
		segment.MinDate().UnixMicro(), // note the time precision
		segment.MaxDate().UnixMicro(), // note the time precision
	)
	if err != nil {
		return segmentId, xerrors.Errorf("unable to check in segments: %w", err)
	}

	// Ingest messages via an appender
	for _, m := range segment.Messages {
		s.incomingSegmentMessage <- appendSegmentMessage{
			0, // message id will be assigned during writing to the db
			int64(segmentId),
			m.Loc.Min,
			m.Loc.Max,
			m.Date.UnixMicro(), // note the time precision
			m.IsTail,
		}
	}

	// Segment terms

	// make these flushes under the terms lock for thread safety
	if len(terms) == 0 {
		// a message is allowed to have no terms:
		// - it could be an empty message
		// - or maybe the literals are too short and thus are not tokenized into terms
		// todo: check how search works in this case (for segments with no terms)
		return segmentId, tx.Commit()
	}

	// 1. get term ids
	// 2. Put term ids + segment to the inverted indexs.termsLock.Lock()

	// note: this is a heavy operation, as it merges existing and new terms together synchronously.
	// it is a bottleneck as it is single threaded service, I could run N terms instances to parallel indexing.
	// but it only is a problem for cold start, when a lot of segments are indexing concurrently,
	// multiple instances management would be more complex anyway.
	tids, err := s.checkInTerms(terms)
	if err != nil {
		return segmentId, err
	}

	for _, tid := range tids {
		s.incomingSegmentTerms <- appendSegmentTerm{tid, uint32(segmentId)}
	}

	// block and confirm both messages and terms are persisted...
	// that is to avoid data corruption on killing the app
	now := time.Now()
	tick := time.NewTicker(time.Millisecond * 10)
	defer tick.Stop()
WaitLoop:
	for {
		select {
		case <-tick.C: // try every tick in case the system is under load
			s.incomingSegmentTermsLastFlushLock.RLock()
			messagesFlushed := len(segment.Messages) > 0 && s.incomingSegmentTermsLastFlush.After(now)
			termsFlushed := len(terms) > 0 && s.incomingSegmentTermsLastFlush.After(now)
			s.incomingSegmentTermsLastFlushLock.RUnlock()
			if messagesFlushed && termsFlushed {
				tick.Stop()
				break WaitLoop
			}
		}
	}

	memoryReportCh <- time.Second * 10 // request for a report

	return segmentId, tx.Commit()
}

// ingestSegmentMessages waits for incoming data to flush to the storage via duckdb appender
func (s *Storage) ingestSegmentMessages(flushInterval time.Duration) {
	var (
		err                  error
		counter, lastFlushed uint64
	)

	flushTick := time.Tick(flushInterval) // how often to flush the appender to the disk
	flush := func() {
		defer func() {
			s.incomingSegmentMessageLastFlush = time.Now()
		}()

		if lastFlushed == counter {
			return
		}

		err = s.segmentMessagesAppender.Flush()
		if err != nil {
			log.Printf("unable to ingest a segment message: %v", err)
		}

		err = s.segmentMessagesTailsAppender.Flush()
		if err != nil {
			log.Printf("unable to ingest a segment message: %v", err)
		}

		lastFlushed = counter
	}

	// read the latest message id, to start from there:
	var lastMessageId int64
	selectSql := `SELECT COALESCE(max(id), 0) FROM file_segments_messages`
	r := s.db.QueryRow(selectSql)
	err = r.Scan(&lastMessageId)
	if err != nil {
		lastMessageId = time.Now().UnixNano()
		log.Printf("fail to read the max message id, revert to unixnano as a seed value: %s", err)
	}

	for {
		select {
		case m := <-s.incomingSegmentMessage:

			// Assigning ids is happening here (a single goroutine) to prevent collisions.
			// it simply increments by one for each new message, starting from some arbitrary value.
			lastMessageId++
			m.messageId = lastMessageId

			err = s.segmentMessagesAppender.AppendRow(m.messageId, m.segmentId, m.locMin, m.locMax, m.dateUnixMicro)
			if err != nil {
				log.Printf("unable to ingest a segment message: %v", err)
			}

			if m.isTail {
				err = s.segmentMessagesTailsAppender.AppendRow(m.messageId)
				if err != nil {
					log.Printf("unable to ingest a segment message tail: %v", err)
				}
			}

			counter++
		case <-flushTick:
			flush()
		}
	}
}

// ingestSegmentTerms waits for incoming data to flush to the storage via duckdb appender
func (s *Storage) ingestSegmentTerms(flushInterval time.Duration) {
	var (
		err                  error
		counter, lastFlushed uint64
	)

	flushTick := time.Tick(flushInterval) // how often to flush the appender to the disk
	flush := func() {
		defer func() {
			s.incomingSegmentTermsLastFlushLock.Lock()
			s.incomingSegmentTermsLastFlush = time.Now()
			s.incomingSegmentTermsLastFlushLock.Unlock()
		}()

		if lastFlushed == counter {
			return
		}

		err = s.segmentTermsAppender.Flush()
		if err != nil {
			log.Printf("unable to ingest a segment term: %v", err)
		}
		lastFlushed = counter
	}

	for {
		select {
		case m := <-s.incomingSegmentTerms:
			err = s.segmentTermsAppender.AppendRow(m.tid, m.sid)
			if err != nil {
				log.Printf("unable to ingest a segment term: %v", err)
			}
			counter++
		case <-flushTick:
			flush()
		}
	}
}

// AllSegmentIds is used to reads all segments for searching with respect to given dates.
// that is used when II is disabled: full-scan
func (s *Storage) AllSegmentIds(minDate, maxDate *time.Time) ([]int, error) {

	where := make([]string, 0, 2)
	if minDate != nil {
		where = append(where, fmt.Sprintf("minDate>=%d", minDate.UnixMicro()))
	}
	if maxDate != nil {
		where = append(where, fmt.Sprintf("maxDate<=%d", maxDate.UnixMicro()))
	}
	if len(where) == 0 {
		where = append(where, "1=1")
	}

	sqlSelect := fmt.Sprintf(
		`SELECT id FROM file_segments WHERE %s ORDER BY dateMin ASC`,
		strings.Join(where, " AND "),
	)

	r, err := s.db.Query(sqlSelect)
	if err != nil {
		return nil, xerrors.Errorf("unable to read existing segments: %w", err)
	}
	defer r.Close()

	var segmentId int
	segmentIds := make([]int, 0, 100)
	for r.Next() {
		err = r.Scan(&segmentId)
		if err != nil {
			return nil, xerrors.Errorf("unable to read existing segments: %w", err)
		}
		segmentIds = append(segmentIds, segmentId)
	}
	return segmentIds, nil
}

/*
SelectLocationsForIndexing splits a file into segments of segmentSize.
Excluding segments that were previously checked in.
*/
func (s *Storage) SelectLocationsForIndexing(file string, segmentSize int64) ([]common.Location, error) {

	fileSize, err := s.GetFileSize(file)
	if errors.Is(err, ErrNoData) {
		return nil, xerrors.Errorf("file %s is not indexed: %w", file, err)
	}

	indexedLocations, err := s.ReadIndexedLocations(common.HashFile(file))
	if err != nil {
		return nil, err
	}

	unindexedLocations := []common.Location{{0, fileSize}}
	for _, indexedLocation := range indexedLocations {
		nextPending := make([]common.Location, 0, len(unindexedLocations))
		for _, pendingLocation := range unindexedLocations {
			nextPending = append(nextPending, pendingLocation.Remove(indexedLocation)...)
		}
		unindexedLocations = nextPending
	}

	// Merge siblings to make contiguous locations
	unindexedLocations = common.MergeSegmentLocations(unindexedLocations)

	// Cut long ones
	result := make([]common.Location, 0, len(unindexedLocations))
	for _, l := range unindexedLocations {
		result = append(result, l.Split(segmentSize)...)
	}

	return result, nil
}

func (s *Storage) ReadIndexedLocations(fileHash common.DataSourceHash) ([]common.Location, error) {
	r, err := s.db.Query("SELECT posFrom, posTo FROM file_segments WHERE fileHash=? ORDER BY posFrom ASC", fileHash)
	if err != nil {
		return nil, xerrors.Errorf("unable to read existing file segments: %w", err)
	}
	defer r.Close()

	indexedLocations := make([]common.Location, 0, 100)
	for r.Next() {
		is := common.Location{}
		err = r.Scan(&is.Min, &is.Max)
		if err != nil {
			return nil, xerrors.Errorf("unable to read existing file segments: %w", err)
		}
		indexedLocations = append(indexedLocations, is)
	}
	return indexedLocations, nil
}

func (s *Storage) GetMessagePage(queryHash string, pageSize, page int, from, to *time.Time) ([]common.MatchedMessage, error) {

	// make sure query exists, to distinguish empty page and missing query
	_, err := s.GetQuerySummary(queryHash, nil, nil)
	if err != nil {
		return nil, err
	}

	// respect sub-query bounds
	timeBound := []string{}
	if from != nil {
		timeBound = append(timeBound, fmt.Sprintf("fsm.date >= %d", from.UnixMicro()))
	}
	if to != nil {
		timeBound = append(timeBound, fmt.Sprintf("fsm.date <= %d", to.UnixMicro()))
	}
	if len(timeBound) == 0 {
		timeBound = append(timeBound, "1=1")
	}

	sqlSelect := `
		SELECT fs.fileHash,q.queryHash,fsm.posFrom,fsm.posTo,fsm.date 
		FROM query_results r
		JOIN queries q ON q.queryHash=r.queryHash
		JOIN file_segments_messages fsm ON fsm.id=r.message_id		    
	    JOIN file_segments fs ON fs.id=fsm.segment_id
		WHERE r.queryHash=? AND %s
		ORDER BY fsm.date ASC 
		LIMIT ? OFFSET ?
		`
	sqlSelect = fmt.Sprintf(sqlSelect, strings.Join(timeBound, " AND "))

	r, err := s.db.Query(sqlSelect, queryHash, pageSize, page*pageSize)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoData
		}
		return nil, xerrors.Errorf("unable to read existing file segments: %w", err)
	}
	defer r.Close()

	messages := make([]common.MatchedMessage, 0, pageSize)
	var date int64
	for r.Next() {
		m := common.MatchedMessage{}

		err = r.Scan(&m.DataSource, &m.QueryHash, &m.Loc.Min, &m.Loc.Max, &date)
		if err != nil {
			return nil, xerrors.Errorf("unable to read existing file segments: %w", err)
		}

		m.Date = time.UnixMicro(date).UTC()

		messages = append(messages, m)
	}

	// touch the query
	_, err = s.db.Exec("UPDATE queries SET lastRead=? WHERE queryHash=?", time.Now().UnixMicro(), queryHash)

	return messages, err
}

func (s *Storage) Close() error {
	return s.db.Close()
}

func (s *Storage) Migrate() (err error) {
	f, err := migrationFS.Open("migrations/1_init.up.sql")
	if err != nil {
		return err
	}
	migrateContent, err := io.ReadAll(f)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(string(migrateContent))
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil // skip migrations
	}
	if err != nil {
		return err
	}
	return nil
}

// CheckInQuery registers a query as running (not complete)
// To mark the query as complete call s.FinishQuery()
func (s *Storage) CheckInQuery(hash string, text string, from *time.Time, to *time.Time) error {
	insertSQL := "INSERT INTO queries(query, queryHash, dateMin, dateMax) VALUES (?,?,?,?)"

	var dateMin, dateMax any
	if from != nil {
		dateMin = from.UnixMicro()
	}
	if to != nil {
		dateMax = to.UnixMicro()
	}

	_, err := s.db.Exec(insertSQL, text, hash, dateMin, dateMax)
	if err != nil {
		return xerrors.Errorf("unable to check in a query: %w", err)
	}

	return nil
}

func (s *Storage) CheckInFinishedQuery(hash string) error {
	updateSQL := "UPDATE queries SET builtDate=? WHERE queryHash=? and builtDate IS NULL" // idempotent modify

	_, err := s.db.Exec(updateSQL, time.Now().UnixMicro(), hash)
	if err != nil {
		return xerrors.Errorf("unable to check in a finished query: %w", err)
	}

	return nil
}

func (s *Storage) EvictQueries() error {
	ttl := time.Hour * 24 * 7
	_, err := s.db.Exec("DELETE FROM queries WHERE lastRead<?", time.Now().Add(-ttl).UnixMicro())
	if err != nil {
		return err
	}
	_, err = s.db.Exec("DELETE FROM query_results WHERE queryHash NOT IN (SELECT queryHash FROM queries)")
	if err != nil {
		return err
	}
	return nil
}

// GetQueriesSummaries returns summary for all queries in the DB
func (s *Storage) GetQueriesSummaries() (summaries []common.QuerySummary, err error) {

	selectSQL := "SELECT queryHash FROM queries ORDER BY builtDate DESC"
	r, err := s.db.Query(selectSQL)
	if err != nil {
		err = xerrors.Errorf("unable to read queries: %w", err)
		return
	}
	defer r.Close()
	var queryHash string
	for r.Next() {
		r.Scan(&queryHash)
		summ, err := s.GetQuerySummary(queryHash, nil, nil)
		if err != nil {
			err = xerrors.Errorf("unable to read query summary: %w", err)
		}
		summaries = append(summaries, summ)
	}

	return
}

func (s *Storage) GetQuerySummary(hash string, from, to *time.Time) (common.QuerySummary, error) {
	summary := common.QuerySummary{
		QueryId: hash,
	}

	// respect sub-query bounds
	timeBound := []string{}
	if from != nil {
		timeBound = append(timeBound, fmt.Sprintf("file_segments_messages.date >= %d", from.UnixMicro()))
	}
	if to != nil {
		timeBound = append(timeBound, fmt.Sprintf("file_segments_messages.date <= %d", to.UnixMicro()))
	}
	if len(timeBound) == 0 {
		timeBound = append(timeBound, "1=1")
	}

	selectSQL := `
	SELECT 
	    query, 
	    dateMin, 
	    dateMax, 
	    builtDate, 
	    min(file_segments_messages.date) as DocMin, 
	    max(file_segments_messages.date) as DocMax, 
	    count(posFrom) as DocCount
	FROM queries
	LEFT JOIN query_results ON query_results.queryHash=queries.queryHash
	LEFT JOIN file_segments_messages ON query_results.message_id=file_segments_messages.id
	WHERE queries.queryHash=? AND %s
	GROUP BY query, dateMin, dateMax, builtDate
`
	selectSQL = fmt.Sprintf(selectSQL, strings.Join(timeBound, " AND "))

	r := s.db.QueryRow(selectSQL, hash)

	var dateMin, dateMax, builtDate, docMin, docMax *int64
	err := r.Scan(&summary.Text, &dateMin, &dateMax, &builtDate, &docMin, &docMax, &summary.Total)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return summary, ErrNoData
		}
		return summary, xerrors.Errorf("query stats: %w", err)
	}

	if dateMin != nil {
		from := time.UnixMicro(*dateMin).UTC()
		summary.From = &from
	}
	if dateMax != nil {
		to := time.UnixMicro(*dateMax).UTC()
		summary.To = &to
	}

	if builtDate != nil {
		built := time.UnixMicro(*builtDate).UTC()
		summary.BuiltAt = &built
		summary.Complete = true
	}

	if docMin != nil {
		docMinDate := time.UnixMicro(*docMin).UTC()
		summary.MinDoc = &docMinDate
	}

	if docMax != nil {
		docMaxDate := time.UnixMicro(*docMax).UTC()
		summary.MaxDoc = &docMaxDate
	}

	return summary, nil
}

// CheckInQueryMessage is merely a scheduler, note it reports no errors regarding ingestion...
// that is for performance considerations.
// A message is a link to a location in a file.
func (s *Storage) CheckInQueryMessage(message common.MatchedMessage) {
	s.incomingQueryMessages <- message
}

// ingestQueryMessages waits for incoming data to flush to the storage via duckdb appender
func (s *Storage) ingestQueryMessages(messageFlushTick time.Duration) {
	var (
		err                  error
		counter, lastFlushed uint64
	)

	flushTick := time.Tick(messageFlushTick) // how often to flush the appender to the disk
	flush := func() {
		if lastFlushed == counter {
			return
		}

		err = s.queryMessageAppender.Flush()
		if err != nil {
			log.Printf("unable to ingest a message: %v", err)
		}
		lastFlushed = counter
	}

	for {
		select {
		case m := <-s.incomingQueryMessages:
			err = s.queryMessageAppender.AppendRow(m.QueryHash, m.Id)
			if err != nil {
				log.Printf("unable to ingest a message: %v", err)
			}
			counter++
		case <-flushTick:
			flush()
		}
	}
}

func (s *Storage) GetSegments(segmentIds []int) ([]common.IndexedSegment, error) {
	if len(segmentIds) == 0 {
		return nil, nil
	}

	segments := make([]common.IndexedSegment, 0, len(segmentIds))

	// Read in one pass.

	segmentsSQL := fmt.Sprintf(
		`
		SELECT file_segments.id, fileHash, file_segments_messages.id, file_segments_messages.posFrom, file_segments_messages.posTo, date FROM file_segments
		JOIN file_segments_messages ON file_segments_messages.segment_id=file_segments.id
		WHERE file_segments.id IN(%s)
		ORDER BY file_segments.dateMin -- oldest first
		`,
		strings.TrimRight(strings.Repeat("?,", len(segmentIds)), ","),
	)
	r, err := s.db.Query(segmentsSQL, common.SliceToAny(segmentIds)...)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var (
		lastSegmentId, segmentId, messageId int64
		posFrom, posTo, date                int64
		lastFileHash, fileHash              common.DataSourceHash
		messages                            []common.IndexedMessage
	)

	for r.Next() {
		err = r.Scan(&segmentId, &fileHash, &messageId, &posFrom, &posTo, &date)
		if err != nil {
			return nil, err
		}
		if lastSegmentId == 0 {
			lastSegmentId = segmentId
			lastFileHash = fileHash
		}

		if lastSegmentId != segmentId {
			segments = append(segments, common.IndexedSegment{lastFileHash, messages})
			messages = make([]common.IndexedMessage, 0)
			lastSegmentId = segmentId
			lastFileHash = fileHash
		}

		messages = append(messages, common.IndexedMessage{
			messageId, common.Location{posFrom, posTo}, time.UnixMicro(date).UTC(), false,
		})
	}

	if len(messages) != 0 {
		segments = append(segments, common.IndexedSegment{fileHash, messages})
	}

	return segments, nil
}

// ReadSegmentIdsFromTerms returns all segment Ids for the terms provided.
// It filters out segments based on min/max segments.
func (s *Storage) ReadSegmentIdsFromTerms(termIds []uint32, minSegment uint64, maxSegment uint64) (segments []uint64, err error) {
	if len(termIds) == 0 {
		return segments, nil
	}

	selectSql := fmt.Sprintf(
		`SELECT DISTINCT(segment_id) FROM file_segments_terms WHERE term_id IN (%s)`,
		strings.TrimRight(strings.Repeat("?,", len(termIds)), ","),
	)
	r, err := s.db.Query(selectSql, common.SliceToAny(termIds)...)
	if err != nil {
		return nil, fmt.Errorf("select term segments fail: %w", err)
	}
	defer r.Close()

	var segmentId uint64
	for r.Next() {
		err = r.Scan(&segmentId)
		if err != nil {
			return nil, fmt.Errorf("read term segments fail: %w", err)
		}
		segments = append(segments, segmentId)
	}

	return segments, nil
}

// ReadSegmentLocationsPerDS is a helper tool to assist in tests.
// This is useful when we only know segment ids and what to cross-check with expected file locations.
func (s *Storage) ReadSegmentLocationsPerDS(segmentIds []int) (
	ret map[common.DataSourceHash][]common.IndexedMessage,
	err error,
) {
	ret = make(map[common.DataSourceHash][]common.IndexedMessage)

	segments, err := s.GetSegments(segmentIds)
	if err != nil {
		return
	}

	for _, ss := range segments {
		for _, m := range ss.Messages {
			m.Id = -1 // in tests we don't care about ids
			ret[ss.DataSource] = append(ret[ss.DataSource], m)
		}
	}

	return
}

func (s *Storage) ReadSegmentsInfo(segmentIds []int) ([]common.IndexedSegmentInfo, error) {
	infos := make([]common.IndexedSegmentInfo, 0, len(segmentIds))

	if len(segmentIds) == 0 {
		return nil, nil
	}

	// Read in one pass.

	segmentsSQL := fmt.Sprintf(
		`
		SELECT fs.id,fs.fileHash,fs.dateMin,fs.dateMax,min(m.posFrom),max(m.posTo),count(m.id) FROM file_segments fs
	    JOIN file_segments_messages m ON m.segment_id=fs.id
		WHERE fs.id IN (%s)
		GROUP BY fs.id,fs.fileHash,fs.dateMin,fs.dateMax
		ORDER BY fs.dateMin -- oldest first
		`,
		strings.TrimRight(strings.Repeat("?,", len(segmentIds)), ","),
	)
	r, err := s.db.Query(segmentsSQL, common.SliceToAny(segmentIds)...)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var (
		segmentId, dateMin, dateMax, posFrom, posTo, messages int64
		fileHash                                              common.DataSourceHash
	)

	for r.Next() {
		err = r.Scan(&segmentId, &fileHash, &dateMin, &dateMax, &posFrom, &posTo, &messages)
		if err != nil {
			return nil, err
		}

		infos = append(infos, common.IndexedSegmentInfo{
			segmentId,
			fileHash,
			time.UnixMicro(dateMin),
			time.UnixMicro(dateMax),
			posFrom,
			posTo,
			messages,
		})
	}

	return infos, nil
}

// ReadTermsLike allows to iterate over all terms in the ii
// Returns a map "rr"->["error","recurring",...]. (but with term ids)
// This lookup is used during the 1st phase of query building, to expand the list of relevant terms.
// The terms in the resulting map are not sorted, but they are deduplicated.
func (s *Storage) ReadTermsLike(likeTerms []string) (map[string][]uint32, error) {
	ret := make(map[string][]uint32)
	for _, lt := range likeTerms {
		ret[lt] = nil
	}

	for _, lt := range likeTerms {
		simTerms, err := s.termsDir.GetMatchedTermIds(func(term string) bool {
			return strings.Contains(term, lt) // <-- note that "similar" means "contains"
		})
		if err != nil {
			return nil, xerrors.Errorf("read similar terms failed: %w", err)
		}
		ret[lt] = append(ret[lt], s.hashTerms(simTerms)...)
	}

	return ret, nil
}

func (s *Storage) GetFileSize(file string) (int64, error) {
	allFiles, err := s.AllFiles()
	if err != nil {
		return 0, err
	}

	fileSize, ok := allFiles[file]
	if !ok {
		return 0, ErrNoData
	}

	return fileSize, nil
}

func (s *Storage) GetDatasourceSize(ds common.DataSourceHash) (size int64, err error) {
	r := s.db.QueryRow("SELECT size FROM files WHERE fileHash=?", ds)
	err = r.Scan(&size)
	return
}

// MergeSegments finds small segments in the storage, and merges them to a bigger ones,
// with respect to the segment size.
// Each small segment will be merged with the next one until the result is > segmentSize.
// It starts with the earliest ones (these segments are likely to be found at the tail a file)
func (s *Storage) MergeSegments(segmentSize int64) (merged bool, err error) {
	mergesCount := 0

	tx, err := s.db.Begin()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	// reset accumulators

	r, err := tx.Query("SELECT id,fileHash,posFrom,posTo,dateMax FROM file_segments WHERE (posTo-posFrom)<? ORDER BY fileHash, posFrom", segmentSize)
	if err != nil {
		return false, err
	}

	shouldMerge := func(prev, next common.IndexedSegmentInfo) bool {
		if (next.To - prev.From) > segmentSize+segmentSize/2 {
			return false // do not merge if new segment becomes too big (150% of the segment size)
		}
		if prev.DataSource != next.DataSource {
			return false // do not merge segments from diff files
		}
		if prev.To != next.From {
			return false // do not merge non-sequential segments
		}
		return true
	}

	// in one pass merge sequential segments
	var (
		curSegment, lastSegment              common.IndexedSegmentInfo
		segmentId, posFrom, posTo, dateMicro int64
		fileHash                             common.DataSourceHash
	)
	for r.Next() {
		err = r.Scan(&segmentId, &fileHash, &posFrom, &posTo, &dateMicro)
		if err != nil {
			return false, xerrors.Errorf("merge segments: read segments failed: %w", err)
		}

		curSegment = common.IndexedSegmentInfo{
			Id:         segmentId,
			DataSource: fileHash,
			From:       posFrom,
			To:         posTo,
			MaxDate:    time.UnixMicro(dateMicro),
		}

		if !shouldMerge(lastSegment, curSegment) {
			lastSegment = curSegment
			continue
		}

		// log.Printf("Merge segment #%d[%d-%d] and #%d[%d-%d]", curSegment.Id, curSegment.From, curSegment.To, lastSegment.Id, lastSegment.From, lastSegment.To)

		// extend
		mergesCount++
		lastSegment.To = curSegment.To
		lastSegment.MaxDate = curSegment.MaxDate
		_, err = tx.Exec(
			"UPDATE file_segments SET posTo=?,dateMax=? WHERE id=?",
			lastSegment.To,
			lastSegment.MaxDate.UnixMicro(),
			lastSegment.Id,
		)
		if err != nil {
			return false, xerrors.Errorf("merge segments: extend segment failed: %w", err)
		}
		// move terms (this can lead to rows duplication for the same terms in both segments)
		_, err = tx.Exec("UPDATE file_segments_terms SET segment_id=? WHERE segment_id=?", lastSegment.Id, curSegment.Id)
		if err != nil {
			return false, xerrors.Errorf("merge segments: move terms failed: %w", err)
		}
		// remove term dups
		_, err = tx.Exec(`
		WITH dups AS (
				SELECT min(rowid) as minRowid, term_id,segment_id
				FROM file_segments_terms
				WHERE segment_id=?
				GROUP BY term_id,segment_id
				HAVING count(term_id)>1
		)
				DELETE FROM file_segments_terms WHERE rowid IN (
				    SELECT fst.rowid FROM file_segments_terms fst
				    JOIN dups USING (term_id,segment_id)
					WHERE fst.rowid > dups.minRowid
				)
		       	
		`, lastSegment.Id)
		if err != nil {
			return false, xerrors.Errorf("merge segments: move terms failed: %w", err)
		}
		// move messages
		_, err = tx.Exec("UPDATE file_segments_messages SET segment_id=? WHERE segment_id=?", lastSegment.Id, curSegment.Id)
		if err != nil {
			return false, xerrors.Errorf("merge segments: move messages failed: %w", err)
		}
		// remove merged segment
		_, err = tx.Exec("DELETE FROM file_segments WHERE id=?", curSegment.Id)
		if err != nil {
			return false, xerrors.Errorf("merge segments: remove merged segment failed: %w", err)
		}
	}

	return mergesCount > 0, tx.Commit()
}

// QueryAggregate returns a map of time intervals To messages within
// keys as unix micro timestamp as a beginning of a period.
// see "unit" values in calculateDiscreteUnit function.
func (s *Storage) QueryAggregate(queryHash, unit string, from, to time.Time) (timeline map[int64]int64, err error) {

	// see https://duckdb.org/docs/sql/functions/dateformat
	aggregationFormat := "%Y-%m-%d %H:%M:%S.%g"
	truncateTimestampUnit := unit
	switch unit {
	case "year":
		aggregationFormat = "%Y"
	case "month":
		aggregationFormat = "%Y-%m"
	case "day":
		aggregationFormat = "%Y-%m-%d"
	case "hour":
		aggregationFormat = "%Y-%m-%d %H"
	case "minute":
		aggregationFormat = "%Y-%m-%d %H:%M"
	case "second":
		aggregationFormat = "%Y-%m-%d %H:%M:%S"
	case "millisecond":
		aggregationFormat = "%Y-%m-%d %H:%M:%S.%g" // fractional second (milli precision)
		truncateTimestampUnit = "milliseconds"
	default:
		return nil, fmt.Errorf("invalid unit provided: %s", unit)
	}

	// log.Printf("Storage aggregate query %s, %s as %s, between %d and %d", queryHash, unit, aggregationFormat, from.UnixMicro(), to.UnixMicro())

	selectSQL := `
SELECT
    strftime(make_timestamp(m.date), ?) as timeFrame, -- for grouping
    epoch_ms(datetrunc(?,make_timestamp(m.date))) as timeFrameStart, -- beginning of the period in ms
    count(*) as count -- count of messages
FROM query_results qr
	JOIN file_segments_messages m ON m.id=qr.message_id
    WHERE queryHash=? AND (m.date BETWEEN ? AND ?)
	GROUP BY timeFrame, timeFrameStart
    ORDER BY timeFrameStart; -- sort by oldest first
	`

	r, err := s.db.Query(selectSQL, aggregationFormat, truncateTimestampUnit, queryHash, from.UnixMicro(), to.UnixMicro())
	if err != nil {
		err = xerrors.Errorf("unable to aggregate message rows: %w", err)
		return
	}
	defer r.Close()

	timeline = make(map[int64]int64)
	var (
		timeFrame, messageCount int64
		timeFrameStr            string
	)
	for r.Next() {
		r.Scan(&timeFrameStr, &timeFrame, &messageCount)
		timeline[timeFrame] = messageCount
	}

	return
}

// hashTerms uses crc32 to hash a term, so each term owns its identity, so we can remove uniqueness checks during ingestion).
// btw, crc32 is well optimized on CPU instructions level and used in many dbs as hashing algo.
// see https://hpi.de/fileadmin/user_upload/fachgebiete/plattner/publications/papers/martinboissier/hash_index_sap_hana_dexa.pdf
func (s *Storage) hashTerms(terms []string) []uint32 {
	tids := make([]uint32, len(terms))
	for i, term := range terms {
		tids[i] = crc32.Checksum([]byte(term), crc32c)
	}
	return tids
}

// checkInTerms is a non-blocking call during ingestion,
// it dumps the terms to FST, and makes term ids via hashes
func (s *Storage) checkInTerms(terms []string) ([]uint32, error) {
	err := s.termsDir.Put(terms)
	if err != nil {
		return nil, xerrors.Errorf("check in terms failed: put terms: %w", err)
	}
	return s.hashTerms(terms), nil
}

// ReportMemory shows usage of Duckdb
func reportMemory(db *sql.DB) {

	printStats := func() {
		var (
			name, dbSize, blockSize, walSize, memSize, memLimit string
			totalBlocks, usedBlocks, freeBlocks                 float64
		)
		r := db.QueryRow(`PRAGMA database_size`)
		err := r.Scan(&name, &dbSize, &blockSize, &totalBlocks, &usedBlocks, &freeBlocks, &walSize, &memSize, &memLimit)
		if err != nil {
			log.Print(err)
			return
		}
		freeBlocksPct := 0.0
		if totalBlocks > 0 {
			freeBlocksPct = freeBlocks / totalBlocks
		}
		log.Printf("DB Stats: size:%s, wal:%s, mem:%s/%s, free:%.02f%% ", dbSize, walSize, memSize, memLimit, freeBlocksPct)

		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		log.Printf(
			"System Stats: %s, %s, %s",
			fmt.Sprintf("HeapAlloc:%vMiB", m.Alloc/1024/1024), // heap only
			fmt.Sprintf("Sys:%vMiB", m.Sys/1024/1024),         // total sys virtual memory
			fmt.Sprintf("NumGC:%v\n", m.NumGC),
		)
	}

	var lastMemoryReport time.Time
	for {
		select {
		case d := <-memoryReportCh:
			tooSoon := time.Now().Sub(lastMemoryReport) < d
			if tooSoon {
				continue // limit throughput
			}
			printStats()
			lastMemoryReport = time.Now()
		}
	}
}

func NewStorage(storagePath string, ingestFlushTick, searchFlushTick time.Duration) (*Storage, error) {
	duckFile := filepath.Join(storagePath, "db.docs")

	termsDirPath := filepath.Join(storagePath, "terms")
	err := os.Mkdir(termsDirPath, 0777)
	if err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	termsDir, err := NewTermsDir(termsDirPath)
	if err != nil {
		return nil, err
	}
	go func() {
		// merge terms files
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				err := termsDir.Merge()
				if err != nil {
					log.Printf("terms merge fail: %s", err)
					time.Sleep(time.Second * 10)
					continue
				}
				err = termsDir.Cleanup()
				if err != nil {
					log.Printf("terms cleanup fail: %s", err)
					time.Sleep(time.Second * 10)
				}
			}
		}
	}()

	// add config values
	duckOptions := map[string]string{
		"memory_limit":               "1GB",
		"temp_directory":             storagePath,
		"immediate_transaction_mode": "true",
	}
	duckFile += "?"
	for k, v := range duckOptions {
		duckFile += fmt.Sprintf("%s=%s&", k, v)
	}

	connector, err := duckdb.NewConnector(duckFile, nil)
	if err != nil {
		panic(err)
	}

	s := &Storage{
		db: sql.OpenDB(connector),

		termsDir:               termsDir,
		appendersFlushInterval: ingestFlushTick,
	}

	err = s.Migrate()
	if err != nil {
		panic(err)
	}

	conn, err := connector.Connect(context.Background())
	if err != nil {
		return nil, err
	}

	appender, err := duckdb.NewAppenderFromConn(conn, "", "query_results")
	if err != nil {
		return nil, err
	}
	s.queryMessageAppender = appender

	appender, err = duckdb.NewAppenderFromConn(conn, "", "file_segments_messages")
	if err != nil {
		return nil, err
	}
	s.segmentMessagesAppender = appender

	appender, err = duckdb.NewAppenderFromConn(conn, "", "file_segments_messages_tail")
	if err != nil {
		return nil, err
	}
	s.segmentMessagesTailsAppender = appender

	s.incomingSegmentMessage = make(chan appendSegmentMessage)
	go s.ingestSegmentMessages(ingestFlushTick)

	appender, err = duckdb.NewAppenderFromConn(conn, "", "file_segments_terms")
	if err != nil {
		return nil, err
	}
	s.segmentTermsAppender = appender
	s.incomingSegmentTerms = make(chan appendSegmentTerm)
	go s.ingestSegmentTerms(ingestFlushTick)

	s.incomingQueryMessages = make(chan common.MatchedMessage)
	go s.ingestQueryMessages(searchFlushTick)

	memoryReportCh = make(chan time.Duration, 10)
	go reportMemory(s.db)

	return s, nil
}
