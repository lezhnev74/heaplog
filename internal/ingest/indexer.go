package ingest

import (
	"context"
	"iter"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"heaplog_2024/internal/common"
)

type task struct {
	file       string
	segmentBuf []byte
	layouts    []MessageLayout
}
type taskResult struct {
	task     task
	tokens   [][]byte
	messages []common.Message
}

// Indexer processes log file segments in parallel, tokenizing content and parsing dates
// using a configurable number of workers
type Indexer struct {
	ctx       context.Context
	blacklist sync.Map
	workers   int
	tokenize  func([]byte) [][]byte
	parseDate func([]byte) (time.Time, error)
	bufPool   *common.BufferPool
	logger    *zap.Logger
}

func NewIndexer(
	ctx context.Context,
	logger *zap.Logger,
	tokenize func(i []byte) [][]byte,
	parseDate func(b []byte) (time.Time, error),
) *Indexer {
	bufPool := common.NewBufferPool([]int{1024})
	return &Indexer{
		ctx:       ctx,
		workers:   1, // predictable results
		bufPool:   bufPool,
		logger:    logger,
		tokenize:  tokenize,
		parseDate: parseDate,
	}
}

// indexSegments processes pending segments from multiple files in parallel and returns an iterator of task results.
func (ix *Indexer) indexSegments(pendingSegments map[string][][]MessageLayout) iter.Seq[taskResult] {

	tasks := ix.produceTasks(pendingSegments)
	tasksResults := ix.consumeTasksViaWorkerPool(tasks)

	return func(yield func(taskResult) bool) {
		for r := range tasksResults {
			ix.bufPool.Put(r.task.segmentBuf)
			r.task.segmentBuf = nil
			if !yield(r) {
				break
			}
		}
	}
}

// consumeTasksViaWorkerPool processes incoming tasks using a pool of workers and returns results through a channel.
// It spawns the configured number of worker goroutines that process tasks in parallel.
// Each worker tokenizes messages, extracts and validates dates, and collects unique terms.
// If date parsing fails for any message in a file, the file is blacklisted and its remaining segments are skipped.
func (ix *Indexer) consumeTasksViaWorkerPool(in <-chan task) <-chan taskResult {
	results := make(chan taskResult)

	// launch workers in a separate goroutine
	go func() {

		wg := sync.WaitGroup{}
		wg.Add(ix.workers)
		defer func() {
			wg.Wait()
			close(results)
		}()

		// workers pool indexes segments in parallel
		for j := 0; j < ix.workers; j++ {
			go func() {
				defer wg.Done()
			TaskLoop:
				for t := range in {
					if _, blacklisted := ix.blacklist.Load(t.file); blacklisted {
						continue // skip faulty files
					}

					// calculate effective position in the buffer by offsetting absolute position
					pos := func(pos int) int { return pos - t.layouts[0].Loc.From }

					// Tokenize each message in the layouts
					messages := make([]common.Message, 0, len(t.layouts))
					termsMap := make(map[string]struct{})
					for _, m := range t.layouts {
						// skip date tokens
						appendTermsUnique(termsMap, ix.tokenize(t.segmentBuf[pos(m.Loc.From):pos(m.DateLoc.From)]))
						appendTermsUnique(termsMap, ix.tokenize(t.segmentBuf[pos(m.DateLoc.To):pos(m.Loc.To)]))

						dateBuf := t.segmentBuf[pos(m.DateLoc.From):pos(m.DateLoc.To)]
						date, err := ix.parseDate(dateBuf)
						if err != nil {
							ix.logger.Error(
								"parse date fail",
								zap.String("file", t.file),
								zap.ByteString("date", dateBuf),
								zap.Error(err),
							)
							ix.blacklist.Store(t.file, nil)
							continue TaskLoop
						}

						messages = append(messages, common.Message{m.Loc, date})
					}

					// Collect unique terms from the messages
					terms := make([][]byte, 0, len(termsMap))
					for term := range termsMap {
						terms = append(terms, []byte(term))
					}
					results <- taskResult{t, terms, messages}
				}
			}()
		}
	}()

	return results
}

// produceTasks creates tasks from pending segments by reading file content in chunks.
// It takes a map of file paths to their segments containing message layouts.
// For each segment, it reads the corresponding bytes from the file using a buffer from the pool.
// Returns a channel of tasks containing file path, segment bytes, and message layouts.
// If file operations fail, the file is blacklisted and skipped.
func (ix *Indexer) produceTasks(pendingSegments map[string][][]MessageLayout) <-chan task {
	tasks := make(chan task)

	// produce tasks in a separate goroutine
	go func() {
		for file, segments := range pendingSegments {
			fd, err := os.Open(file)
			if err != nil {
				ix.logger.Warn("open file", zap.String("file", file), zap.Error(err))
				continue
			}

			// make a new scope here
			func() {
				defer fd.Close()

				for _, segment := range segments {

					// Expect hang-up:
					select {
					case <-ix.ctx.Done():
						return
					default:
					}

					segmentLoc := common.Location{segment[0].Loc.From, segment[len(segment)-1].Loc.To}
					bytes := ix.bufPool.Get(segmentLoc.Len())[:segmentLoc.Len()]
					_, err = fd.ReadAt(bytes, int64(segmentLoc.From))
					if err != nil {
						ix.logger.Error("read layouts", zap.String("file", file), zap.Error(err))
						ix.blacklist.Store(file, nil)
						continue
					}
					tasks <- task{file, bytes, segment}
				}
			}()
		}
		close(tasks)
	}()

	return tasks
}
