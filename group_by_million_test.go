package chai_test

import (
	"database/sql"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	_ "github.com/chaisql/chai"
	"github.com/chaisql/chai/internal/kv"
	"github.com/stretchr/testify/require"
)

// peakRSSMB returns the peak RSS (high water mark) of the current process.
func peakRSSMB(t testing.TB) float64 {
	t.Helper()

	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		t.Skip("cannot read /proc/self/status")
	}
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "VmHWM:") {
			f := strings.Fields(l)
			kb, err := strconv.Atoi(f[1])
			require.NoError(t, err)
			return float64(kb) / 1024
		}
	}
	t.Skip("VmHWM not found")
	return 0
}

// TestGroupByMillionRows runs GROUP BY queries over one million rows and checks
// that results are correct, that the peak memory stays bounded (the intermediate
// results must be computed in batches and spilled, not held in memory), and that
// no transient data is left behind once the queries are done.
func TestGroupByMillionRows(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping million rows test in short mode")
	}

	dir := t.TempDir()

	db, err := sql.Open("chai", dir)
	require.NoError(t, err)

	_, err = db.Exec("CREATE TABLE t(a int, b int, PRIMARY KEY (a))")
	require.NoError(t, err)

	const n = 1_000_000

	tx, err := db.Begin()
	require.NoError(t, err)
	st, err := tx.Prepare("INSERT INTO t VALUES ($1, $2)")
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		_, err := st.Exec(i, i*7)
		require.NoError(t, err)
	}
	require.NoError(t, st.Close())
	require.NoError(t, tx.Commit())

	runtime.GC()
	peakBefore := peakRSSMB(t)

	// group one million rows into one thousand groups
	rows, err := db.Query("SELECT a % 1000 AS k, COUNT(*), SUM(b) FROM t GROUP BY a % 1000")
	require.NoError(t, err)
	groups := 0
	for rows.Next() {
		var k, cnt, sum int64
		require.NoError(t, rows.Scan(&k, &cnt, &sum))
		// rows i in [0, 1e6) with i % 1000 == k are i = k + 1000*j for j in [0, 1000)
		require.EqualValues(t, 1000, cnt)
		require.EqualValues(t, 7*(1000*k+499_500_000), sum)
		groups++
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, 1000, groups)

	// group one million rows into one million groups
	rows, err = db.Query("SELECT a AS k, COUNT(*), SUM(b) FROM t GROUP BY a")
	require.NoError(t, err)
	groups = 0
	var totalCnt, totalSum int64
	for rows.Next() {
		var k, cnt, sum int64
		require.NoError(t, rows.Scan(&k, &cnt, &sum))
		require.EqualValues(t, 1, cnt)
		require.EqualValues(t, 7*k, sum)
		totalCnt += cnt
		totalSum += sum
		groups++
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, n, groups)
	require.EqualValues(t, n, totalCnt)
	require.EqualValues(t, 7*(n-1)*n/2, totalSum)

	runtime.GC()
	peakAfter := peakRSSMB(t)

	// the intermediate results of the aggregation must be spilled in batches:
	// the peak memory must stay bounded no matter how many rows are grouped
	require.Less(t, peakAfter-peakBefore, 512.0,
		"peak memory grew by %.1fMB during aggregation (before: %.1fMB, after: %.1fMB)",
		peakAfter-peakBefore, peakBefore, peakAfter)

	require.NoError(t, db.Close())

	// transient data produced by the aggregations must have been cleaned up:
	// reopening the database must not fail with "namespace already in use"
	// and no leftover transient keys should remain in the store.
	db, err = sql.Open("chai", dir)
	require.NoError(t, err)
	rows, err = db.Query("SELECT a % 1000 AS k, COUNT(*) FROM t GROUP BY a % 1000")
	require.NoError(t, err)
	cnt := 0
	for rows.Next() {
		cnt++
	}
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())
	require.Equal(t, 1000, cnt)
	require.NoError(t, db.Close())

	eng, err := kv.NewEngine(dir, kv.Options{
		RollbackSegmentNamespace: 1,
		MinTransientNamespace:    10_000,
		MaxTransientNamespace:    11_000,
	})
	require.NoError(t, err)
	defer eng.Close()

	sess := eng.NewTransientSession()
	it, err := sess.Iterator(nil)
	require.NoError(t, err)
	defer it.Close()
	keys := 0
	for valid := it.Start(false); valid; valid = it.Move(false) {
		keys++
	}
	require.NoError(t, it.Error())
	// only the table rows and a handful of catalog keys should remain
	require.LessOrEqual(t, keys, n+1000, "transient aggregation data was not cleaned up")
}
