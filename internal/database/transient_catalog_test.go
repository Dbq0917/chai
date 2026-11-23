package database_test

import (
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/pkg/atomic"
	"github.com/chaisql/chai/internal/tree"
	"github.com/stretchr/testify/require"
)

type fakeTransientLifecycle struct {
	inUse map[uint64]struct{}
}

func (f *fakeTransientLifecycle) IsTransientNamespaceInUse(namespace uint64) bool {
	_, ok := f.inUse[namespace]
	return ok
}

// TestGetFreeTransientNamespaceSkipsInUse ensures that when the transient
// counter wraps around, namespaces still owned by a long-running transaction
// are skipped instead of being handed out a second time.
func TestGetFreeTransientNamespaceSkipsInUse(t *testing.T) {
	const minNS, maxNS = 100, 105

	catalog := database.NewCatalog()
	catalog.TransientNamespaces = atomic.NewCounter(minNS, maxNS, true)

	lifecycle := &fakeTransientLifecycle{inUse: map[uint64]struct{}{}}
	catalog.SetTransientNamespaceLifecycle(lifecycle)

	// fill the whole range and mark every namespace as in use.
	allocated := make(map[tree.Namespace]bool)
	for range maxNS - minNS {
		ns := catalog.GetFreeTransientNamespace()
		require.False(t, allocated[ns], "namespace %d allocated twice", ns)
		allocated[ns] = true
		lifecycle.inUse[uint64(ns)] = struct{}{}
	}

	// the counter is now about to wrap: release only two namespaces and make
	// sure those are the only ones returned afterwards.
	delete(lifecycle.inUse, 101)
	delete(lifecycle.inUse, 103)

	seen := map[tree.Namespace]bool{}
	for range 4 {
		ns := catalog.GetFreeTransientNamespace()
		require.Contains(t, []tree.Namespace{101, 103}, ns)
		seen[ns] = true
	}
	require.Len(t, seen, 2)
}

// TestGetFreeTransientNamespaceWithoutLifecycle keeps the historical
// behaviour when no lifecycle tracker is wired (e.g. a catalog built
// directly in unit tests).
func TestGetFreeTransientNamespaceWithoutLifecycle(t *testing.T) {
	catalog := database.NewCatalog()
	catalog.TransientNamespaces = atomic.NewCounter(100, 103, true)

	require.Equal(t, tree.Namespace(101), catalog.GetFreeTransientNamespace())
	require.Equal(t, tree.Namespace(102), catalog.GetFreeTransientNamespace())
	require.Equal(t, tree.Namespace(100), catalog.GetFreeTransientNamespace())
}
