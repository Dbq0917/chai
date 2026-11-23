package kv

import (
	"github.com/chaisql/chai/internal/engine"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
)

var _ engine.Session = (*BatchSession)(nil)

var (
	tombStone = []byte{0}
)

type BatchSession struct {
	Store           *PebbleEngine
	DB              *pebble.DB
	Batch           *pebble.Batch
	snapshot        *snapshot
	written         bool
	closed          bool
	rollbackSegment *RollbackSegment
	maxBatchSize    int
}

func (s *PebbleEngine) NewBatchSession() engine.Session {
	// Take a snapshot before creating the batch. Reads performed before the
	// first write use this snapshot, so they cannot observe intermediate
	// commits made by another write session.
	sn := s.acquireSharedSnapshot()

	b := s.db.NewIndexedBatch()

	return &BatchSession{
		Store:           s,
		DB:              s.db,
		Batch:           b,
		snapshot:        sn,
		rollbackSegment: s.rollbackSegment,
		maxBatchSize:    s.opts.MaxBatchSize,
	}
}

func (s *BatchSession) Commit() error {
	if s.closed {
		return errors.New("already closed")
	}

	// We are about to commit the batch, we can empty
	// the rollback segment.
	err := s.rollbackSegment.Clear(s.Batch)
	if err != nil {
		return err
	}

	err = s.Batch.Commit(nil)
	if err != nil {
		return err
	}

	return s.Close()
}

func (s *BatchSession) Close() error {
	if s.closed {
		return errors.New("already closed")
	}
	s.closed = true

	err := s.Batch.Close()
	if snapshotErr := s.Store.releaseSharedSnapshot(s.snapshot); err == nil {
		err = snapshotErr
	}

	return err
}

// Get returns a value associated with the given key. If not found, returns ErrKeyNotFound.
func (s *BatchSession) Get(k []byte) ([]byte, error) {
	if !s.written {
		return get(s.snapshot.snapshot, k)
	}

	return get(s.Batch, k)
}

// Exists returns whether a key exists and is visible by the current session.
func (s *BatchSession) Exists(k []byte) (bool, error) {
	if !s.written {
		return exists(s.snapshot.snapshot, k)
	}

	return exists(s.Batch, k)
}

func (s *BatchSession) applyBatch() error {
	if s.Batch.Empty() {
		return nil
	}

	err := s.rollbackSegment.Apply(s.Batch)
	if err != nil {
		return err
	}

	// this is an intermediary commit that might be rolled back by the user
	// so we don't need durability here.
	err = s.Batch.Commit(pebble.NoSync)
	if err != nil {
		return err
	}

	// reset batch
	s.Batch.Reset()

	return nil
}

func (s *BatchSession) ensureBatchSize() error {
	if s.Batch.Len() < s.maxBatchSize {
		return nil
	}

	// The batch is too large. Insert the rollback segments and commit the batch.
	return s.applyBatch()
}

// Insert inserts a key-value pair. If it already exists, it returns ErrKeyAlreadyExists.
func (s *BatchSession) Insert(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	ok, err := exists(s.Batch, k)
	if err != nil {
		return err
	}
	if ok {
		return engine.ErrKeyAlreadyExists
	}

	err = s.Batch.Set(k, v, nil)
	if err != nil {
		return err
	}

	s.written = true

	return s.ensureBatchSize()
}

// Put stores a key value pair. If it already exists, it overrides it.
func (s *BatchSession) Put(k, v []byte) error {
	if len(k) == 0 {
		return errors.New("cannot store empty key")
	}

	if len(v) == 0 {
		return errors.New("cannot store empty value")
	}

	err := s.Batch.Set(k, v, nil)
	if err != nil {
		return err
	}

	s.written = true

	return s.ensureBatchSize()
}

// Delete a record by key. If the key doesn't exist, it doesn't do anything.
func (s *BatchSession) Delete(k []byte) error {
	err := s.Batch.Delete(k, nil)
	if err != nil {
		return err
	}

	s.written = true

	return s.ensureBatchSize()
}

// DeleteRange deletes all keys in the given range.
// This implementation deletes all keys one by one to simplify the rollback.
func (s *BatchSession) DeleteRange(start []byte, end []byte) error {
	err := s.Batch.DeleteRange(start, end, nil)
	if err != nil {
		return err
	}

	s.written = true

	return s.ensureBatchSize()
}

func (s *BatchSession) Iterator(opts *engine.IterOptions) (engine.Iterator, error) {
	var popts *pebble.IterOptions
	if opts != nil {
		popts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}

	var it *pebble.Iterator
	var err error
	if s.written {
		it, err = s.Batch.NewIter(popts)
	} else {
		it, err = s.snapshot.snapshot.NewIter(popts)
	}
	if err != nil {
		return nil, err
	}

	return &iterator{
		Iterator: it,
	}, err
}
