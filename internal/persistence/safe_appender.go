package persistence

import (
	"database/sql/driver"

	"github.com/marcboeker/go-duckdb/v2"
)

type Appender struct {
	a        *duckdb.Appender
	appendCh chan []driver.Value
	flushCh  chan struct{}
}

func NewAppender(duckAppender *duckdb.Appender) *Appender {
	a := Appender{
		a:        duckAppender,
		appendCh: make(chan []driver.Value),
		flushCh:  make(chan struct{}),
	}
	go func() {
		for {
			select {
			case args := <-a.appendCh:
				if len(args) == 0 {
					return
				}
				err := a.a.AppendRow(args...)
				if err != nil {
					panic(err)
				}
			case <-a.flushCh:
				err := a.a.Flush()
				if err != nil {
					panic(err)
				}
			}
		}
	}()
	return &a
}

func (a *Appender) AppendRow(args ...driver.Value) error {
	a.appendCh <- args
	return nil
}
func (a *Appender) Flush() error {
	a.flushCh <- struct{}{}
	return nil
}

func (a *Appender) Close() error {
	a.a.Flush() // sync flush to make sure buffers are persisted
	a.appendCh <- []driver.Value(nil)
	return a.a.Close()
}
