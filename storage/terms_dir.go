package storage

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"golang.org/x/xerrors"
	"log"
	"os"
	"path"
	"slices"
	"strings"
	"time"
)

// this is a gateway to work with multiple terms files.
// It allows quick ingestion by creating new files, reading from files and merging small ones.
// It must enforce invariant of unique term ids, but the same terms must have the same id in all files.

type TermsDir struct {
	dir        string
	mainList   *TermsFileList
	mergedList *TermsFileList

	fsts      []*vellum.FST
	maxTermId uint64
}

// Put must assign unique ids for all new terms, so it must synchronise access
func (d *TermsDir) Put(terms []string) (ids []uint64, err error) {
	ids = make([]uint64, 0, len(terms))
	newTerms := make([]termId, 0, len(terms))

	// sync to allocate unique ids:
	d.mainList.safeWrite(func() {
		var (
			tid uint64
			ok  bool
		)
		for _, term := range terms {

			// read term id
			for _, tf := range d.mainList.files {
				tid, ok, err = tf.fst.Get([]byte(term))
				if ok {
					break
				}
			}

			if err != nil {
				return
			}
			if ok {
				// term exists, skip
				ids = append(ids, tid)
				continue
			}

			d.maxTermId++ // allocate a new unique id
			newTerms = append(newTerms, termId{term, d.maxTermId})
			ids = append(ids, d.maxTermId)
		}
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to read a term: %w", err)
	}

	if len(newTerms) == 0 {
		return // all terms are already in the system
	}

	// Build a new FST:

	slices.SortFunc(newTerms, func(a, b termId) int { return cmp.Compare(a.term, b.term) }) // prepare for FST

	buf := bytes.NewBuffer(nil)
	b, err := vellum.New(buf, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed FST: %w", err)
	}
	for _, n := range newTerms {
		err = b.Insert([]byte(n.term), n.id)
		if err != nil {
			return nil, xerrors.Errorf("failed FST: %w", err)
		}
	}
	err = b.Close()
	if err != nil {
		return nil, xerrors.Errorf("failed FST: %w", err)
	}

	err = d.newTermsFileFromFSTBytes(buf.Bytes())
	if err != nil {
		err = xerrors.Errorf("failed to add a new fst file: %w", err)
	}

	return
}

// All is only used in tests for assertion
func (d *TermsDir) All() (all []termId, err error) {
	d.mainList.safeRead(func() {
		var it vellum.Iterator
		for _, tf := range d.mainList.files {
			it, err = tf.fst.Iterator(nil, nil)
			if err != nil {
				return
			}
			for err == nil {
				term, id := it.Current()
				all = append(all, termId{string(term), id})
				err = it.Next()
			}
			if errors.Is(err, vellum.ErrIteratorDone) {
				err = nil
			}
			it.Close()
		}
	})

	if err == nil {
		slices.SortFunc(all, func(a, b termId) int { return cmp.Compare(a.term, b.term) })
		all = slices.Compact(all)
	}

	return
}

func (d *TermsDir) Cleanup() error {
	d.mergedList.safeWrite(func() {
		for _, mf := range d.mergedList.files {
			os.Remove(mf.path)
		}
		d.mergedList.files = d.mergedList.files[:0]
	})
	return nil
}

func (d *TermsDir) Merge() error {

	t1 := time.Now()

	// select files:
	mergeFiles := make([]*termsFile, 10)
	d.mainList.safeRead(func() {
		n := copy(mergeFiles[:], d.mainList.files[:])
		mergeFiles = mergeFiles[:n]
	})

	if len(mergeFiles) < 2 {
		return nil // nothing to merge
	}

	// make new fst:
	fstBuf := bytes.NewBuffer(nil)
	b, err := vellum.New(fstBuf, nil)
	if err != nil {
		return xerrors.Errorf("merge fail: %w", err)
	}

	var totalTerms int
	for _, tf := range mergeFiles {
		totalTerms += tf.fst.Len()
	}

	tree := go_iterators.NewSliceIterator([]termId{})
	defer tree.Close()
	for _, tf := range mergeFiles {
		it, err := tf.fst.Iterator(nil, nil)
		if err != nil {
			it.Close()
			return xerrors.Errorf("merge fail: %w", err)
		}

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

	for {
		tid, err := tree.Next()
		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		} else if err != nil {
			return xerrors.Errorf("merge fail: %w", err)
		}

		err = b.Insert([]byte(tid.term), tid.id)
		if err != nil {
			return xerrors.Errorf("merge fail: %w", err)
		}
	}

	err = b.Close()
	if err != nil {
		return xerrors.Errorf("failed FST: %w", err)
	}

	// update index:
	err = d.newTermsFileFromFSTBytes(fstBuf.Bytes())
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

	log.Printf("merged %d terms files in %s", len(mergeFiles), time.Now().Sub(t1).String())

	return nil
}

// internal method used in adding and merging terms as FST
func (d *TermsDir) newTermsFileFromFSTBytes(buf []byte) error {
	fst, err := vellum.Load(buf)
	if err != nil {
		return xerrors.Errorf("failed FST: %w", err)
	}

	// Write a file
	filename := path.Join(d.dir, fmt.Sprintf("%d.fst", time.Now().UnixNano()))
	err = os.WriteFile(filename, buf, 0666)
	if err != nil {
		return xerrors.Errorf("failed writing term file: %w", err)
	}

	// Insert to the main list:
	d.mainList.safeWrite(func() {
		d.mainList.putFile(&termsFile{
			path: filename,
			len:  int64(len(buf)),
			fst:  fst,
		})
	})

	return nil
}

func (d *TermsDir) GetMatchedTermIds(match func(term string) bool) (ids []uint64, err error) {

	fsts := make([]*vellum.FST, 0)
	d.mainList.safeRead(func() {
		for _, tf := range d.mainList.files {
			fsts = append(fsts, tf.fst)
		}
	})

	var it vellum.Iterator
	for _, fst := range fsts {
		it, err = fst.Iterator(nil, nil)
		if err != nil {
			return
		}
		for err == nil {
			term, id := it.Current()
			if match(string(term)) {
				ids = append(ids, id)
			}
			err = it.Next()
		}
		if errors.Is(err, vellum.ErrIteratorDone) {
			err = nil
		}
		it.Close()
	}
	slices.Sort(ids)
	ids = slices.Compact(ids)

	return ids, nil
}

func NewTermsDir(dir string) (*TermsDir, error) {
	d := &TermsDir{
		dir:        dir,
		mainList:   NewTermsList(),
		mergedList: NewTermsList(),
	}

	// load fsts
	entry, err := os.ReadDir(dir)
	if err != nil {
		return nil, xerrors.Errorf("unable to read terms from %s: %w", dir, err)
	}
	for _, e := range entry {
		if e.IsDir() {
			continue
		}

		if !strings.HasSuffix(e.Name(), ".fst") {
			continue
		}

		// load fst
		fstFile := path.Join(dir, e.Name())
		fb, err := os.ReadFile(fstFile)
		if err != nil {
			return nil, xerrors.Errorf("unable to read terms from %s: %w", fstFile, err)
		}
		fst, err := vellum.Load(fb)
		if err != nil {
			return nil, xerrors.Errorf("unable to load terms from %s: %w", fstFile, err)
		}
		d.mainList.putFile(&termsFile{
			path: fstFile,
			len:  int64(len(fb)),
			fst:  fst,
		})
	}

	// find max id
	for _, tf := range d.mainList.files {
		it, err := tf.fst.Iterator(nil, nil)
		if err != nil {
			it.Close()
			return nil, xerrors.Errorf("failed FST: %w", err)
		}
		for err == nil {
			_, tid := it.Current()
			d.maxTermId = max(d.maxTermId, tid)
			err = it.Next()
		}
		it.Close()
	}

	return d, nil
}