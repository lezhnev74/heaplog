package terms

import (
	"bytes"
	"cmp"
	"database/sql"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"golang.org/x/xerrors"
	"io"
	"log"
	"slices"
	"time"
)

// this is a gateway to work with multiple terms files (actually blobs).
// It allows quick ingestion by creating new files, reading from files and merging small ones.
// It must enforce invariant of unique term ids, but the same terms must have the same id in all files.

type TermsDir struct {
	db         *sql.DB // duckdb connection
	mainList   *TermsFileList
	mergedList *TermsFileList
}

// Put must assign unique ids for all new terms, so it must synchronise access
func (d *TermsDir) Put(terms []string) (err error) {
	if len(terms) == 0 {
		return nil
	}

	return d.writeNewFst(func(w io.Writer) error {
		// Build a new FST:
		slices.Sort(terms) // prepare for FST
		b, err := vellum.New(w, nil)
		if err != nil {
			return xerrors.Errorf("failed FST: %w", err)
		}
		for _, term := range terms {
			err = b.Insert([]byte(term), 0)
			if err != nil {
				return xerrors.Errorf("failed FST: %w", err)
			}
		}
		err = b.Close()
		if err != nil {
			return xerrors.Errorf("failed FST: %w", err)
		}
		return nil
	})
}

// All is only used in tests for assertion
func (d *TermsDir) All() (all []string, err error) {
	d.mainList.safeRead(func() {
		var it vellum.Iterator
		var fst *vellum.FST
		for _, tf := range d.mainList.files {

			fst, err = d.loadFst(tf)
			if err != nil {
				return
			}
			defer fst.Close()

			it, err = fst.Iterator(nil, nil)
			if err != nil {
				return
			}
			defer it.Close()
			for err == nil {
				term, _ := it.Current()
				all = append(all, string(term))
				err = it.Next()
			}
			if errors.Is(err, vellum.ErrIteratorDone) {
				err = nil
			}
		}
	})

	if err == nil {
		slices.Sort(all)
		all = slices.Compact(all)
	}

	return
}

func (d *TermsDir) Cleanup() (err error) {
	d.mergedList.safeWrite(func() {
		for _, mf := range d.mergedList.files {
			_, err = d.db.Exec(`DELETE FROM fst WHERE name=?`, mf.path)
		}
		d.mergedList.files = d.mergedList.files[:0]
	})
	return
}

func (d *TermsDir) Merge() error {
	// select files:
	mergeFiles := make([]*termsFile, 20)
	d.mainList.safeRead(func() {
		n := copy(mergeFiles[:], d.mainList.files[:])
		mergeFiles = mergeFiles[:n]
	})

	if len(mergeFiles) < 2 {
		return nil // nothing to merge
	}

	log.Printf("Start merging %d FST", len(mergeFiles))

	t := time.Now()
	err := d.writeNewFst(func(w io.Writer) error {

		// Build a selection tree from multiple FSTs:
		tree := go_iterators.NewSliceIterator([]termId{})
		defer tree.Close()

		for _, tf := range mergeFiles {
			fst, err := d.loadFst(tf)
			if err != nil {
				return xerrors.Errorf("merge fail: %w", err)
			}
			defer fst.Close()

			it, err := fst.Iterator(nil, nil)
			if err != nil {
				return xerrors.Errorf("merge fail: %w", err)
			}
			defer it.Close()

			fileIt := go_iterators.NewCallbackIterator[termId](
				func() (termId, error) {
					if errors.Is(err, vellum.ErrIteratorDone) {
						return termId{}, go_iterators.EmptyIterator
					} else if err != nil {
						return termId{}, err
					}
					tb, tv := it.Current()
					stb := string(tb)
					err = it.Next()
					return termId{stb, tv}, nil
				},
				func() error {
					return it.Close()
				},
			)
			tree = go_iterators.NewUniqueSelectingIterator(tree, fileIt, func(a, b termId) int {
				return cmp.Compare(a.term, b.term)
			})
		}

		// Stream to a new fst:
		mergedFst, err := vellum.New(w, nil)
		if err != nil {
			return xerrors.Errorf("merge fail: %w", err)
		}
		defer mergedFst.Close()

		for {
			tid, err := tree.Next()
			if errors.Is(err, go_iterators.EmptyIterator) {
				break
			} else if err != nil {
				return xerrors.Errorf("merge fail: %w", err)
			}

			err = mergedFst.Insert([]byte(tid.term), tid.id)
			if err != nil {
				return xerrors.Errorf("merge fail: %w", err)
			}
		}

		return nil
	})
	if err != nil {
		err = xerrors.Errorf("failed to add a new fst file: %w", err)
	}

	// move to the merged list
	d.mainList.safeWrite(func() {
		for _, mf := range mergeFiles {
			for k, af := range d.mainList.files {
				if mf != af {
					continue
				}

				d.mergedList.safeWrite(func() {
					d.mergedList.files = append(d.mergedList.files, mf)
				})
				d.mainList.files = append(d.mainList.files[:k], d.mainList.files[k+1:]...)
			}
		}
	})

	log.Printf("merged %d term files in %.02fs", len(mergeFiles), time.Now().Sub(t).Seconds())
	return nil
}

func (d *TermsDir) GetMatchedTermIds(match func(term string) bool) (terms []string, err error) {

	fsts := make([]*vellum.FST, 0)
	d.mainList.safeRead(func() {
		for _, tf := range d.mainList.files {
			fst, _ := d.loadFst(tf)
			if err != nil {
				return
			}
			fsts = append(fsts, fst)
		}
	})

	var it vellum.Iterator
	for _, fst := range fsts {
		it, err = fst.Iterator(nil, nil)
		if err != nil {
			return
		}
		for err == nil {
			term, _ := it.Current()
			if match(string(term)) {
				terms = append(terms, string(term))
			}
			err = it.Next()
		}
		if errors.Is(err, vellum.ErrIteratorDone) {
			err = nil
		}
		it.Close()
		fst.Close()
	}
	slices.Sort(terms)
	terms = slices.Compact(terms)

	return terms, nil
}

// writeNewFst prepares a callback with the Writer,
// after the callback return it closes the writer.
func (d *TermsDir) writeNewFst(writeF func(w io.Writer) error) error {

	// Prepare
	fstName := fmt.Sprintf("%d.fst", time.Now().UnixNano())
	var w bytes.Buffer

	// Write
	err := writeF(&w)
	if err != nil {
		return err
	}

	// Flush
	_, err = d.db.Exec(`INSERT INTO fst(name,fst) VALUES(?,?)`, fstName, w.Bytes())
	if err != nil {
		return err
	}

	// Insert to the main list:
	d.mainList.safeWrite(func() {
		d.mainList.putFile(&termsFile{
			path: fstName,
			len:  int64(w.Len()),
		})
	})

	return nil
}

func (d *TermsDir) StartMergeMonitor() {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			err := d.Merge()
			if err != nil {
				log.Printf("terms merge fail: %s", err)
				time.Sleep(time.Second * 10)
				continue
			}
			err = d.Cleanup()
			if err != nil {
				log.Printf("terms cleanup fail: %s", err)
				time.Sleep(time.Second * 10)
			}
		}
	}
}

func (d *TermsDir) loadFst(tf *termsFile) (*vellum.FST, error) {
	var fstBytes []byte
	r := d.db.QueryRow(`SELECT fst FROM fst WHERE name=?`, tf.path)
	if r.Err() != nil {
		return nil, r.Err()
	}
	err := r.Scan(&fstBytes)
	if err != nil {
		return nil, err
	}

	return vellum.Load(fstBytes)
}

func NewTermsDir(db *sql.DB) (*TermsDir, error) {
	d := &TermsDir{
		db:         db,
		mainList:   NewTermsList(),
		mergedList: NewTermsList(),
	}

	// load fsts
	r, err := db.Query(`SELECT name,octet_length(fst) FROM fst`)
	if err != nil {
		return nil, xerrors.Errorf("unable to read existing FSTs: %w", err)
	}
	defer r.Close()

	var fstName string
	var fstLen int64
	for r.Next() {
		err = r.Scan(&fstName, &fstLen)
		d.mainList.putFile(&termsFile{
			path: fstName,
			len:  fstLen,
		})
	}

	return d, nil
}
