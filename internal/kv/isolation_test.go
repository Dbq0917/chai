package kv_test

import (
	"testing"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDirtyRead(t *testing.T) {
	ng := testutil.NewEngine(t)
	writer := ng.NewBatchSession()
	defer writer.Close()

	key := encoding.EncodeInt(nil, 1)
	require.NoError(t, writer.Put(key, []byte("old")))
	// force an intermediate commit after each write
	for i := 0; i < 10; i++ {
		require.NoError(t, writer.Put(encoding.EncodeInt(nil, int64(i+2)), []byte("padding-padding-padding-padding-padding-padding-padding-padding-padding")))
	}

	reader := ng.NewSnapshotSession()
	defer reader.Close()
	v, err := reader.Get(key)
	if err == nil {
		t.Fatalf("dirty read: got %q", v)
	}
	require.Error(t, err)
}

func TestDirtyReadInBatchSession(t *testing.T) {
	ng := testutil.NewEngine(t)
	writer := ng.NewBatchSession()
	defer writer.Close()

	key := encoding.EncodeInt(nil, 1)
	require.NoError(t, writer.Put(key, []byte("old")))
	for i := 0; i < 10; i++ {
		require.NoError(t, writer.Put(encoding.EncodeInt(nil, int64(i+2)), []byte("padding-padding-padding-padding-padding-padding-padding-padding-padding")))
	}

	reader := ng.NewBatchSession()
	defer reader.Close()
	v, err := reader.Get(key)
	if err == nil {
		t.Fatalf("dirty read: got %q", v)
	}
	require.Error(t, err)
}

func TestBatchSessionReadsOwnWrites(t *testing.T) {
	ng := testutil.NewEngine(t)
	session := ng.NewBatchSession()
	defer session.Close()

	key := encoding.EncodeInt(nil, 1)
	require.NoError(t, session.Put(key, []byte("new")))

	v, err := session.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), v)
}
