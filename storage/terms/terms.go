package terms

import (
	"bytes"
	"cmp"
	"errors"
	"github.com/blevesearch/vellum"
	go_iterators "github.com/lezhnev74/go-iterators"
	"golang.org/x/xerrors"
	"io"
	"log"
	"slices"
)

// Terms is a storage for all indexed terms. It uses FST for compact storage.
// Each term has unique uint64 sequential id.
// It supports persistence on disk by command from the outside.
type Terms struct {
	fst    *vellum.FST // old terms in FST
	fstBuf *bytes.Buffer
	maxId  uint64
}

type termId struct {
	term string
	id   uint64
}

// Put merges new terms with the existing FST and returns term ids
// note that ids returned in a different order
func (t *Terms) Put(terms []string) ([]uint64, error) {
	slices.Sort(terms)
	_, ids, err := t.rebuild(terms)
	if err != nil {
		return nil, xerrors.Errorf("terms rebuild: %w", err)
	}
	return ids, nil
}

// All returns iterator of existing terms
func (t *Terms) All() go_iterators.Iterator[termId] {

	if t.fst == nil {
		return go_iterators.NewSliceIterator[termId]([]termId{})
	}

	it, err := t.fst.Iterator(nil, nil)
	if err != nil {
		log.Fatalf("fst read fail: %s", err)
	}

	it2 := go_iterators.NewCallbackIterator[termId](
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

	return it2
}

func (t *Terms) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write(t.fstBuf.Bytes())
	return int64(n), err
}

// Get returns id of the term
func (t *Terms) Get(term string) (uint64, bool) {
	if t.fst != nil {
		v, ok, err := t.fst.Get([]byte(term))
		if err != nil {
			log.Fatalf("get %s: %s", term, err)
		}
		return v, ok
	}

	return 0, false
}

// rebuild joins terms and existing fst
// it assigns missing ids for new terms and returns ids
func (t *Terms) rebuild(terms []string) ([]byte, []uint64, error) {
	fstBuf := bytes.NewBuffer(nil)
	termIds := make([]uint64, 0, len(terms))

	b, err := vellum.New(fstBuf, nil)
	if err != nil {
		return nil, nil, xerrors.Errorf("fst create failed: %w", err)
	}

	// process new terms
	newTermIds := make([]termId, 0, len(terms))
	for _, term := range terms {
		id, ok := t.Get(term)
		if ok {
			termIds = append(termIds, id)
			continue
		}

		t.maxId++
		newTermIds = append(newTermIds, termId{term, t.maxId})
		termIds = append(termIds, t.maxId)
	}

	// join current terms + new terms
	termsIt := go_iterators.NewSliceIterator(newTermIds)
	compoundIt := go_iterators.NewUniqueSelectingIterator(t.All(), termsIt, func(a, b termId) int { return cmp.Compare(a.term, b.term) })
	defer compoundIt.Close()

	for {
		tid, err := compoundIt.Next()

		if errors.Is(err, go_iterators.EmptyIterator) {
			break
		} else if err != nil {
			return nil, nil, xerrors.Errorf("iterate next failed: %w", err)
		}

		err = b.Insert([]byte(tid.term), tid.id)
		if err != nil {
			return nil, nil, xerrors.Errorf("fst insert failed: %w", err)
		}
	}

	err = b.Close()
	if err != nil {
		return nil, nil, xerrors.Errorf("fst close failed: %w", err)
	}

	// update terms storage:
	t.fst, err = vellum.Load(fstBuf.Bytes())
	if err != nil {
		err = xerrors.Errorf("fst load failed: %w", err)
	}
	t.fstBuf = fstBuf

	return fstBuf.Bytes(), termIds, err
}

func NewTerms() *Terms {
	return &Terms{
		fstBuf: bytes.NewBuffer(nil),
	}
}

func NewTermsFrom(r io.Reader) (*Terms, error) {

	t := NewTerms()

	// read FST from the reader
	buf := bytes.NewBuffer(nil)
	_, err := buf.ReadFrom(r)
	if err != nil {
		return nil, err
	}

	t.fst, err = vellum.Load(buf.Bytes())
	if err != nil {
		return nil, err
	}

	it, err := t.fst.Iterator(nil, nil)
	if err != nil {
		return nil, xerrors.Errorf("fst max key failed: %w", err)
	}
	defer it.Close()

	for err == nil {
		_, termId := it.Current()
		t.maxId = max(t.maxId, termId)
		err = it.Next()
	}

	return t, nil
}
