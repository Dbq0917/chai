package kv

import (
	"bytes"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/chaisql/chai/internal/encoding"
	"github.com/chaisql/chai/internal/pkg/atomic"
	"github.com/chaisql/chai/internal/pkg/pebbleutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const (
	defaultMaxBatchSize              = 10 * 1024 * 1024 // 10MB
	defaultMaxTransientBatchSize int = 1 << 19          // 512KB
)

type PebbleEngine struct {
	db              *pebble.DB
	opts            Options
	rollbackSegment *RollbackSegment

	// holds the shared snapshot read by all the read sessions
	// when a write session is open.
	// when no write session is open, the snapshot is nil
	// and every read session will use db.NewSnapshot()
	sharedSnapshot struct {
		sync.RWMutex

		snapshot *snapshot
	}

	minTransientNamespace uint64
	maxTransientNamespace uint64

	// transientLifecycle tracks the transient namespaces that are currently
	// in use. It prevents a namespace from being reclaimed while a transient
	// tree (or an iterator reading from it) is still alive, and makes cleanup
	// deterministic once the tree is released.
	transientLifecycle struct {
		sync.Mutex
		inUse map[uint64]struct{}
	}
}

type Options struct {
	RollbackSegmentNamespace int64
	MaxBatchSize             int
	MaxTransientBatchSize    int
	MinTransientNamespace    uint64
	MaxTransientNamespace    uint64
}

func NewEngineWith(path string, opts Options, popts *pebble.Options) (*PebbleEngine, error) {
	if popts == nil {
		popts = &pebble.Options{}
	}

	popts.FormatMajorVersion = pebble.FormatValueSeparation
	popts.Comparer = DefaultComparer
	if popts.Logger == nil {
		popts.Logger = pebbleutil.NoopLoggerAndTracer{}
	}

	popts.EnsureDefaults()

	db, err := pebble.Open(path, popts)
	if err != nil {
		return nil, err
	}

	return NewStore(db, opts), nil
}

func NewEngine(path string, opts Options) (*PebbleEngine, error) {
	var popts pebble.Options
	var pbpath string

	if path == ":memory:" {
		popts.FS = vfs.NewMem()
	} else {
		path = strings.TrimSpace(path)
		path = filepath.Clean(path)
		if path == "" {
			return nil, errors.New("path cannot be empty")
		}

		fi, err := os.Stat(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return nil, err
			}

			err = os.MkdirAll(path, 0700)
			if err != nil {
				return nil, err
			}
		} else {
			if !fi.IsDir() {
				return nil, errors.New("path must be a directory")
			}
		}

		pbpath = filepath.Join(path, "pebble")
	}

	return NewEngineWith(pbpath, opts, &popts)
}

// DefaultComparer is the default implementation of the Comparer interface for chai.
var DefaultComparer = &pebble.Comparer{
	Compare: func(a, b []byte) int {
		an := encoding.Split(a)
		bn := encoding.Split(b)
		if prefixCmp := bytes.Compare(a[:an], b[:bn]); prefixCmp != 0 {
			return prefixCmp
		}
		return encoding.Compare(a[an:], b[bn:])
	},
	Equal:          encoding.Equal,
	AbbreviatedKey: encoding.AbbreviatedKey,
	FormatKey:      pebble.DefaultComparer.FormatKey,
	Separator:      encoding.Separator,
	Successor: func(dst, a []byte) []byte {
		return a
	},
	Split:                encoding.Split,
	ComparePointSuffixes: encoding.Compare,
	CompareRangeSuffixes: encoding.Compare,
	// This name is part of the C++ Level-DB implementation's default file
	// format, and should not be changed.
	Name: "leveldb.BytewiseComparator",
}

func NewStore(db *pebble.DB, opts Options) *PebbleEngine {
	if opts.MaxBatchSize <= 0 {
		opts.MaxBatchSize = defaultMaxBatchSize
	}
	if opts.MaxTransientBatchSize <= 0 {
		opts.MaxTransientBatchSize = defaultMaxTransientBatchSize
	}
	if opts.MinTransientNamespace == 0 {
		panic("min transient namespace cannot be 0")
	}
	if opts.MaxTransientNamespace == 0 {
		panic("max transient namespace cannot be 0")
	}

	engine := &PebbleEngine{
		db:              db,
		opts:            opts,
		rollbackSegment: NewRollbackSegment(db, opts.RollbackSegmentNamespace),
	}
	engine.transientLifecycle.inUse = make(map[uint64]struct{})
	return engine
}

func (s *PebbleEngine) Close() error {
	return s.db.Close()
}

func (s *PebbleEngine) Rollback() error {
	return s.rollbackSegment.Rollback()
}

func (s *PebbleEngine) Recover() error {
	return s.rollbackSegment.Reset()
}

func (s *PebbleEngine) LockSharedSnapshot() {
	s.sharedSnapshot.Lock()
	s.sharedSnapshot.snapshot = &snapshot{
		snapshot: s.db.NewSnapshot(),
		refCount: atomic.NewCounter(0, math.MaxInt64, false),
	}
	s.sharedSnapshot.snapshot.Incr()
	s.sharedSnapshot.Unlock()
}

func (s *PebbleEngine) UnlockSharedSnapshot() {
	s.sharedSnapshot.Lock()
	s.sharedSnapshot.snapshot.Done()
	s.sharedSnapshot.snapshot = nil
	s.sharedSnapshot.Unlock()
}

func (s *PebbleEngine) DB() *pebble.DB {
	return s.db
}

func (s *PebbleEngine) CleanupTransientNamespaces() error {
	return s.db.DeleteRange(
		encoding.EncodeUint(nil, uint64(s.minTransientNamespace)),
		encoding.EncodeUint(nil, uint64(s.maxTransientNamespace)),
		pebble.NoSync,
	)
}

// RegisterTransientNamespace marks a transient namespace as in use.
// A registered namespace is excluded from any range cleanup until it is
// released, so data still being read by a long-running transaction cannot
// be reclaimed underneath it.
func (s *PebbleEngine) RegisterTransientNamespace(namespace uint64) {
	s.transientLifecycle.Lock()
	s.transientLifecycle.inUse[namespace] = struct{}{}
	s.transientLifecycle.Unlock()
}

// IsTransientNamespaceInUse reports whether a transient namespace is
// currently registered, i.e. whether a transient tree is still alive.
func (s *PebbleEngine) IsTransientNamespaceInUse(namespace uint64) bool {
	s.transientLifecycle.Lock()
	_, ok := s.transientLifecycle.inUse[namespace]
	s.transientLifecycle.Unlock()
	return ok
}

// ReleaseTransientNamespace unregisters a transient namespace and reclaims
// the whole key range it occupies. The delete is applied directly to the
// engine (outside of any user transaction), so the memory is returned as
// soon as the transient tree is closed instead of surviving until the
// database restarts.
func (s *PebbleEngine) ReleaseTransientNamespace(namespace uint64) error {
	s.transientLifecycle.Lock()
	_, ok := s.transientLifecycle.inUse[namespace]
	if ok {
		delete(s.transientLifecycle.inUse, namespace)
	}
	s.transientLifecycle.Unlock()

	if !ok {
		return nil
	}

	return s.db.DeleteRange(
		encoding.EncodeUint(nil, namespace),
		encoding.EncodeUint(nil, namespace+1),
		pebble.NoSync,
	)
}

// CleanupUnregisteredTransientNamespaces reclaims every transient namespace
// that is not currently in use. In-use namespaces are skipped by deleting
// around them, which keeps long-running transient trees intact.
// It is used on startup to remove data left over by a previous process that
// did not get a chance to release its transient trees.
func (s *PebbleEngine) CleanupUnregisteredTransientNamespaces() error {
	s.transientLifecycle.Lock()
	inUse := make([]uint64, 0, len(s.transientLifecycle.inUse))
	for ns := range s.transientLifecycle.inUse {
		inUse = append(inUse, ns)
	}
	s.transientLifecycle.Unlock()
	sort.Slice(inUse, func(i, j int) bool { return inUse[i] < inUse[j] })

	start := s.minTransientNamespace
	for _, ns := range inUse {
		if ns > start {
			if err := s.db.DeleteRange(
				encoding.EncodeUint(nil, start),
				encoding.EncodeUint(nil, ns),
				pebble.NoSync,
			); err != nil {
				return err
			}
		}
		start = ns + 1
	}

	if start < s.maxTransientNamespace {
		return s.db.DeleteRange(
			encoding.EncodeUint(nil, start),
			encoding.EncodeUint(nil, s.maxTransientNamespace),
			pebble.NoSync,
		)
	}

	return nil
}
