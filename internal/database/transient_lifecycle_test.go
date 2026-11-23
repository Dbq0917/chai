package database_test

import (
	"context"
	"testing"

	"github.com/chaisql/chai/internal/database"
	"github.com/chaisql/chai/internal/encoding"
	"github.com/chaisql/chai/internal/engine"
	"github.com/chaisql/chai/internal/environment"
	"github.com/chaisql/chai/internal/testutil"
	"github.com/stretchr/testify/require"
)

func testutilParam(v int) environment.Param {
	return environment.Param{Value: v}
}

// countEngineTransientKeys scans the engine transient namespace range.
// It mirrors the production range defined in internal/database/catalog.go.
func countEngineTransientKeys(t *testing.T, ng engine.Engine) int {
	t.Helper()

	sess := ng.NewSnapshotSession()
	defer sess.Close()

	it, err := sess.Iterator(&engine.IterOptions{
		LowerBound: encoding.EncodeUint(nil, uint64(database.MinTransientNamespace)),
		UpperBound: encoding.EncodeUint(nil, uint64(database.MaxTransientNamespace)),
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

// TestTransientTablesReclaimedAfterQueries runs a batch-style workload that
// creates a transient tree for every query (unindexed ORDER BY goes through
// TempTreeSort). It verifies that after 100 iterations no transient data
// remains in the engine: memory used by temporary tables must return to its
// baseline instead of growing query after query.
func TestTransientTablesReclaimedAfterQueries(t *testing.T) {
	db, tx, cleanup := testutil.NewTestTx(t)
	defer cleanup()

	require.NoError(t, testutil.Exec(db, tx, "CREATE TABLE batch_run(id INT PRIMARY KEY, payload INT)"))

	for i := 0; i < 500; i++ {
		require.NoError(t, testutil.Exec(
			db, tx,
			"INSERT INTO batch_run (id, payload) VALUES ($1, $2)",
			testutilParam(i), testutilParam(500-i),
		))
	}

	require.Equal(t, 0, countEngineTransientKeys(t, db.Engine))

	const queryCount = 100
	for i := 0; i < queryCount; i++ {
		res, err := testutil.Query(db, tx, "SELECT id FROM batch_run ORDER BY payload DESC")
		require.NoError(t, err)

		rows := 0
		require.NoError(t, res.Iterate(func(_ database.Row) error {
			rows++
			return nil
		}))
		require.Equal(t, 500, rows)
		require.NoError(t, res.Close())

		// every query releases its transient tree before starting the next one.
		require.Equal(t, 0, countEngineTransientKeys(t, db.Engine))
	}

	// final footprint must match the pre-workload baseline.
	require.Equal(t, 0, countEngineTransientKeys(t, db.Engine))
}

// TestTransientTableSurvivesLongRunningTransaction holds a transient tree
// open for the whole test while other queries run on top of it. The tree of
// the long-running query must remain readable and must not be reclaimed or
// reassigned until its iterator is closed.
func TestTransientTableSurvivesLongRunningTransaction(t *testing.T) {
	db, tx, cleanup := testutil.NewTestTx(t)
	defer cleanup()

	require.NoError(t, testutil.Exec(db, tx, "CREATE TABLE long_run(id INT PRIMARY KEY, weight INT)"))
	for i := 0; i < 100; i++ {
		require.NoError(t, testutil.Exec(
			db, tx,
			"INSERT INTO long_run (id, weight) VALUES ($1, $2)",
			testutilParam(i), testutilParam(100-i),
		))
	}

	longRes, err := testutil.Query(db, tx, "SELECT id FROM long_run ORDER BY weight")
	require.NoError(t, err)

	longIt, err := longRes.Iterator()
	require.NoError(t, err)

	// read a few rows, then leave the iterator open while other batch queries
	// create their own transient trees.
	seen := 0
	for seen < 10 && longIt.Next() {
		seen++
	}
	require.NoError(t, longIt.Error())
	require.Equal(t, 10, seen)

	for range 20 {
		res, err := testutil.Query(db, tx, "SELECT id FROM long_run ORDER BY weight DESC")
		require.NoError(t, err)
		require.NoError(t, res.Skip(context.Background()))
		require.NoError(t, res.Close())
	}

	// the long-running iterator must still be able to consume the rest.
	for longIt.Next() {
		seen++
	}
	require.NoError(t, longIt.Error())
	require.Equal(t, 100, seen)
	require.NoError(t, longIt.Close())
	require.NoError(t, longRes.Close())

	// once the long query is closed, nothing transient remains.
	require.Equal(t, 0, countEngineTransientKeys(t, db.Engine))
}
