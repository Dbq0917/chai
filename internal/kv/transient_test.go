package kv_test

import (
	"testing"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/chaisql/chai/internal/engine"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/chaisql/chai/internal/tree"
	"github.com/chaisql/chai/internal/types"
	"github.com/stretchr/testify/require"
)

const (
	testMinTransientNamespace = 10_000
	testMaxTransientNamespace = 11_000
)

// countTransientKeys scans the whole transient namespace range and returns
// the number of keys still present in the engine.
func countTransientKeys(t *testing.T, ng engine.Engine) int {
	t.Helper()

	sess := ng.NewSnapshotSession()
	defer sess.Close()

	it, err := sess.Iterator(&engine.IterOptions{
		LowerBound: encoding.EncodeUint(nil, testMinTransientNamespace),
		UpperBound: encoding.EncodeUint(nil, testMaxTransientNamespace),
	})
	require.NoError(t, err)
	defer it.Close()

	var count int
	for it.First(); it.Valid(); it.Next() {
		count++
	}
	require.NoError(t, it.Error())
	return count
}

func transientKey(ns tree.Namespace) *tree.Key {
	return tree.NewKey(types.NewBigintValue(int64(ns)))
}

// TestTransientNamespaceReclaimedOnCleanup verifies that transient data is
// deterministically reclaimed when the transient tree cleanup runs, instead
// of surviving until the database restarts.
func TestTransientNamespaceReclaimedOnCleanup(t *testing.T) {
	ng := testutil.NewEngine(t)

	require.Equal(t, 0, countTransientKeys(t, ng))

	const tableCount = 100
	cleanups := make([]func() error, 0, tableCount)

	for i := 0; i < tableCount; i++ {
		ns := tree.Namespace(testMinTransientNamespace + uint64(i))
		sess := ng.NewTransientSession()

		tr, cleanup, err := tree.NewTransient(ng, sess, ns, 0)
		require.NoError(t, err)

		for j := 0; j < 10; j++ {
			require.NoError(t, tr.Put(
				tree.NewKey(types.NewIntegerValue(int32(j))),
				[]byte("FOO"),
			))
		}

		// the namespace must be protected for the whole lifetime of the tree.
		require.True(t, ng.IsTransientNamespaceInUse(uint64(ns)))
		require.NoError(t, sess.(interface{ Flush() error }).Flush())
		cleanups = append(cleanups, cleanup)
	}

	// all the tables are alive: nothing must have been reclaimed yet.
	require.Equal(t, tableCount*10, countTransientKeys(t, ng))

	for _, cleanup := range cleanups {
		require.NoError(t, cleanup())
	}

	// every transient table has been released: the engine must be back to its
	// initial footprint regardless of how many tables were created.
	require.Equal(t, 0, countTransientKeys(t, ng))
}

// TestTransientNamespaceProtectedWhileInUse ensures that releasing one tree
// never touches the data of another tree that is still being read.
func TestTransientNamespaceProtectedWhileInUse(t *testing.T) {
	ng := testutil.NewEngine(t)

	firstNS := tree.Namespace(testMinTransientNamespace)
	secondNS := tree.Namespace(testMinTransientNamespace + 1)

	first, firstCleanup, err := tree.NewTransient(ng, ng.NewTransientSession(), firstNS, 0)
	require.NoError(t, err)
	second, secondCleanup, err := tree.NewTransient(ng, ng.NewTransientSession(), secondNS, 0)
	require.NoError(t, err)

	require.NoError(t, first.Put(transientKey(firstNS), []byte("FIRST")))
	require.NoError(t, second.Put(transientKey(secondNS), []byte("SECOND")))

	firstIt, err := first.Iterator(nil)
	require.NoError(t, err)

	// release the second tree while an iterator is open on the first one.
	require.NoError(t, secondCleanup())
	require.False(t, ng.IsTransientNamespaceInUse(uint64(secondNS)))
	require.True(t, ng.IsTransientNamespaceInUse(uint64(firstNS)))

	// the long-lived iterator must still see its data.
	require.True(t, firstIt.First())
	v, err := firstIt.Value()
	require.NoError(t, err)
	require.Equal(t, []byte("FIRST"), v)
	require.NoError(t, firstIt.Close())

	require.NoError(t, firstCleanup())
	require.Equal(t, 0, countTransientKeys(t, ng))
}

// TestTransientNamespaceCleanupIdempotent makes sure a cleanup function can
// be called safely more than once, mirroring how stream iterators may be
// closed along several error paths.
func TestTransientNamespaceCleanupIdempotent(t *testing.T) {
	ng := testutil.NewEngine(t)

	ns := tree.Namespace(testMinTransientNamespace)
	tr, cleanup, err := tree.NewTransient(ng, ng.NewTransientSession(), ns, 0)
	require.NoError(t, err)
	require.NoError(t, tr.Put(transientKey(ns), []byte("FOO")))

	require.NoError(t, cleanup())
	require.NoError(t, cleanup())
	require.False(t, ng.IsTransientNamespaceInUse(uint64(ns)))
	require.Equal(t, 0, countTransientKeys(t, ng))
}

// TestCleanupUnregisteredTransientNamespaces verifies that a bulk cleanup
// skips namespaces that are still in use and reclaims everything else.
func TestCleanupUnregisteredTransientNamespaces(t *testing.T) {
	ng := testutil.NewEngine(t)

	keepNS := tree.Namespace(testMinTransientNamespace + 2)
	keep, keepCleanup, err := tree.NewTransient(ng, ng.NewTransientSession(), keepNS, 0)
	require.NoError(t, err)
	require.NoError(t, keep.Put(transientKey(keepNS), []byte("KEEP")))

	dropNS := tree.Namespace(testMinTransientNamespace + 5)
	drop, dropCleanup, err := tree.NewTransient(ng, ng.NewTransientSession(), dropNS, 0)
	require.NoError(t, err)
	require.NoError(t, drop.Put(transientKey(dropNS), []byte("DROP")))

	// release the second tree, then write stale data under the same namespace
	// without registering it to emulate leftover on-disk state.
	require.NoError(t, dropCleanup())
	abandoned := ng.NewTransientSession()
	defer abandoned.Close()
	require.NoError(t, tree.New(abandoned, dropNS, 0).Put(transientKey(dropNS), []byte("STALE")))

	require.NoError(t, ng.CleanupUnregisteredTransientNamespaces())

	// the registered tree keeps its data, the stale one is reclaimed.
	require.True(t, ng.IsTransientNamespaceInUse(uint64(keepNS)))
	it, err := keep.Iterator(nil)
	require.NoError(t, err)
	require.True(t, it.First())
	require.NoError(t, it.Close())

	require.False(t, ng.IsTransientNamespaceInUse(uint64(dropNS)))
	require.NoError(t, keepCleanup())
	require.Equal(t, 0, countTransientKeys(t, ng))
}
