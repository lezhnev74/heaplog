package storage

import (
	"bufio"
	"cmp"
	"errors"
	"fmt"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"golang.org/x/xerrors"
	"io"
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

	fsts []*vellum.FST
}

// Put must assign unique ids for all new terms, so it must synchronise access
func (d *TermsDir) Put(terms []string) (err error) {
	if len(terms) == 0 {
		return nil
	}

	return d.writeNewFile(func(w io.Writer) error {
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
		for _, tf := range d.mainList.files {
			fst, err := vellum.Open(tf.path)
			if err != nil {
				return
			}
			it, err = fst.Iterator(nil, nil)
			if err != nil {
				return
			}
			for err == nil {
				term, _ := it.Current()
				all = append(all, string(term))
				err = it.Next()
			}
			if errors.Is(err, vellum.ErrIteratorDone) {
				err = nil
			}
			it.Close()
			fst.Close()
		}
	})

	if err == nil {
		slices.Sort(all)
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

	// select files:
	mergeFiles := make([]*termsFile, 10)
	d.mainList.safeRead(func() {
		n := copy(mergeFiles[:], d.mainList.files[:])
		mergeFiles = mergeFiles[:n]
	})

	if len(mergeFiles) < 2 {
		return nil // nothing to merge
	}

	t := time.Now()
	err := d.writeNewFile(func(w io.Writer) error {

		// Build a selection tree from multiple FSTs:
		tree := go_iterators.NewSliceIterator([]termId{})
		defer tree.Close()

		for _, tf := range mergeFiles {
			fst, err := vellum.Open(tf.path)
			if err != nil {
				return xerrors.Errorf("merge fail: %w", err)
			}
			defer fst.Close()

			it, err := fst.Iterator(nil, nil)
			if err != nil {
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
			fst, _ := vellum.Open(tf.path)
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

// writeNewFile prepares a callback with the Writer,
// after the callback return it closes the writer.
func (d *TermsDir) writeNewFile(writeF func(w io.Writer) error) error {

	// Prepare
	targetFilePath := path.Join(d.dir, fmt.Sprintf("%d.fst", time.Now().UnixNano()))
	f, err := os.Create(targetFilePath)
	if err != nil {
		return xerrors.Errorf("terms: unable to put to a file: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.Remove(targetFilePath) // cleanup
		}
	}()

	w := bufio.NewWriterSize(f, 4096*100)

	// Write
	err = writeF(w)
	if err != nil {
		return err
	}

	// Complete writing
	err2 := w.Flush()
	if err2 != nil {
		log.Printf("unable to wrire a terms file: %s", err2)
	}

	var fStat os.FileInfo
	fStat, err = f.Stat()
	err = f.Close()
	if err != nil {
		log.Printf("unable to close a terms file: %s", err2)
		return err
	}

	// Insert to the main list:
	d.mainList.safeWrite(func() {
		d.mainList.putFile(&termsFile{
			path: targetFilePath,
			len:  fStat.Size(),
		})
	})

	return nil
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
		d.mainList.putFile(&termsFile{
			path: fstFile,
			len:  int64(len(fb)),
		})
	}

	return d, nil
}
