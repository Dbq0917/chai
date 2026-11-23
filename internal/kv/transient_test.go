package kv_test

import (
	"testing"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/chaisql/chai/internal/engine"
	"github.com/chaisql/chai/internal/kv"
	"github.com/stretchr/testify/require"
)

func newTransientEngine(t testing.TB, dir string) *kv.PebbleEngine {
	t.Helper()

	eng, err := kv.NewEngine(dir, kv.Options{
		RollbackSegmentNamespace: 1,
		MinTransientNamespace:    10_000,
		MaxTransientNamespace:    11_000,
		// use a small batch size so that tests don't need to write
		// a lot of data to trigger batch commits
		MaxTransientBatchSize: 1 << 10, // 1KB
	})
	require.NoError(t, err)

	return eng
}

func transientKey(ns uint64, i uint64) []byte {
	k := encoding.EncodeUint(nil, ns)
	return encoding.EncodeUint(k, i)
}

func countTransientKeys(t testing.TB, eng *kv.PebbleEngine) int {
	t.Helper()

	sess := eng.NewTransientSession()
	it, err := sess.Iterator(nil)
	require.NoError(t, err)
	defer it.Close()

	n := 0
	for valid := it.Start(false); valid; valid = it.Move(false) {
		n++
	}
	require.NoError(t, it.Error())

	return n
}

// Ensure that data written through a transient session is visible to iterators
// even after it has been flushed to the store in batches, and that DeleteRange
// removes both buffered and flushed data.
func TestTransientSessionSpill(t *testing.T) {
	eng := newTransientEngine(t, t.TempDir())
	defer eng.Close()

	s := eng.NewTransientSession()

	// write enough data to trigger multiple batch commits
	const n = 100
	val := make([]byte, 128)
	for i := 0; i < n; i++ {
		require.NoError(t, s.Put(transientKey(10_000, uint64(i)), val))
	}

	// the iterator must see all the keys, whether they are still
	// buffered in the batch or already committed to the store
	it, err := s.Iterator(nil)
	require.NoError(t, err)
	count := 0
	for valid := it.Start(false); valid; valid = it.Move(false) {
		count++
	}
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
	require.Equal(t, n, count)

	// Get, Exists and Delete must work on keys already committed to the store
	// as well as on keys still buffered in the batch.
	// With a 1KB batch and 128B values, the first keys have been flushed already.
	v, err := s.Get(transientKey(10_000, 0))
	require.NoError(t, err)
	require.Len(t, v, len(val))

	ok, err := s.Exists(transientKey(10_000, 0))
	require.NoError(t, err)
	require.True(t, ok)

	require.NoError(t, s.Delete(transientKey(10_000, 0)))
	ok, err = s.Exists(transientKey(10_000, 0))
	require.NoError(t, err)
	require.False(t, ok)

	// DeleteRange must remove everything, including committed batches
	require.NoError(t, s.DeleteRange(transientKey(10_000, 0), transientKey(10_001, 0)))
	require.Equal(t, 0, countTransientKeys(t, eng))

	require.NoError(t, s.Close())
}

// Closing a transient session on which nothing was written must not fail.
func TestTransientSessionCloseEmpty(t *testing.T) {
	eng := newTransientEngine(t, t.TempDir())
	defer eng.Close()

	s := eng.NewTransientSession()
	require.NoError(t, s.Close())
}

// Transient data must not survive an engine restart: namespaces are reused
// after a restart and leftover data would make transient tree creation fail.
func TestCleanupTransientNamespaces(t *testing.T) {
	dir := t.TempDir()

	eng := newTransientEngine(t, dir)
	s := eng.NewTransientSession()
	val := make([]byte, 128)
	for i := 0; i < 100; i++ {
		require.NoError(t, s.Put(transientKey(10_000, uint64(i)), val))
	}
	require.NoError(t, s.Close())
	require.NotEqual(t, 0, countTransientKeys(t, eng))
	require.NoError(t, eng.Close())

	eng = newTransientEngine(t, dir)
	defer eng.Close()
	require.NoError(t, eng.CleanupTransientNamespaces())
	require.Equal(t, 0, countTransientKeys(t, eng))
}

// Get on a missing key must return ErrKeyNotFound.
func TestTransientSessionGetNotFound(t *testing.T) {
	eng := newTransientEngine(t, t.TempDir())
	defer eng.Close()

	s := eng.NewTransientSession()
	_, err := s.Get(transientKey(10_000, 0))
	require.ErrorIs(t, err, engine.ErrKeyNotFound)

	require.NoError(t, s.Put(transientKey(10_000, 1), []byte("v")))
	_, err = s.Get(transientKey(10_000, 0))
	require.ErrorIs(t, err, engine.ErrKeyNotFound)
	require.NoError(t, s.Close())
}
