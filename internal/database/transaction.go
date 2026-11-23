package database

import (
	"sync"
	"time"

	"github.com/chaisql/chai/internal/engine"
	"github.com/cockroachdb/errors"
)

// Transaction represents a database transaction. It provides methods for managing the
// collection of tables and the transaction itself.
// Transaction is either read-only or read/write. Read-only can be used to read tables
// and read/write can be used to read, create, delete and modify tables.
type Transaction struct {
	db   *Database
	conn *Connection

	// Timestamp at which the transaction was created.
	// The timestamp must use the local timezone.
	TxStart time.Time

	Session   engine.Session
	Engine    engine.Engine
	ID        uint64
	Writable  bool
	WriteTxMu *sync.Mutex
	writeMode bool
	isolation IsolationLevel
	// these functions are run after a successful rollback.
	OnRollbackHooks []func()
	// these functions are run after a successful commit.
	OnCommitHooks []func()

	Catalog       *Catalog
	catalogWriter *CatalogWriter
}

func (tx *Transaction) Connection() *Connection {
	return tx.conn
}

// BeginWrite upgrades a writable transaction to a batch session.
// The write lock is acquired lazily so read-only work performed in a
// writable transaction never has to wait for, or interfere with, a
// concurrent writer.
func (tx *Transaction) BeginWrite() error {
	if !tx.Writable || tx.writeMode {
		return nil
	}

	tx.WriteTxMu.Lock()

	// Another writer may have committed while this transaction was reading.
	// Use the latest catalog for the write portion of the transaction.
	if tx.catalogWriter == nil {
		tx.Catalog = tx.db.Catalog()
	}

	batch := tx.Engine.NewBatchSession()
	if err := tx.Session.Close(); err != nil {
		_ = batch.Close()
		tx.WriteTxMu.Unlock()
		return err
	}

	tx.Session = batch
	tx.writeMode = true

	return nil
}

// Rollback the transaction. Can be used safely after commit.
func (tx *Transaction) Rollback() error {
	var err error

	if tx.writeMode {
		// Keep the shared snapshot available while the intermediate commits
		// are rolled back, otherwise readers could observe uncommitted data.
		tx.db.txmu.Lock()
		defer tx.db.txmu.Unlock()

		err = tx.Engine.Rollback()
		if closeErr := tx.Session.Close(); err == nil {
			err = closeErr
		}

		tx.WriteTxMu.Unlock()
		tx.writeMode = false
	} else {
		err = tx.Session.Close()
	}

	if err != nil {
		return err
	}

	for i := len(tx.OnRollbackHooks) - 1; i >= 0; i-- {
		tx.OnRollbackHooks[i]()
	}

	return nil
}

// Commit the transaction. Calling this method on read-only transactions
// will return an error.
func (tx *Transaction) Commit() error {
	if !tx.Writable {
		return errors.New("cannot commit read-only transaction")
	}

	// lock the transaction mutex to prevent any other transaction
	// from being created while the commit is in progress.
	tx.db.txmu.Lock()
	defer tx.db.txmu.Unlock()

	if tx.writeMode {
		defer func() {
			tx.WriteTxMu.Unlock()
		}()
	}

	var err error
	if tx.writeMode {
		err = tx.Session.Commit()
		if err == nil {
			_ = tx.Session.Close()
		}
	} else {
		err = tx.Session.Close()
	}
	if err != nil {
		return err
	}

	tx.writeMode = false

	for i := len(tx.OnCommitHooks) - 1; i >= 0; i-- {
		tx.OnCommitHooks[i]()
	}

	// if the catalog has been modified, update the database catalog
	if tx.catalogWriter != nil {
		tx.db.SetCatalog(tx.Catalog)
	}

	return nil
}

func (tx *Transaction) CatalogWriter() *CatalogWriter {
	if !tx.Writable {
		panic("cannot get catalog writer from read-only transaction")
	}

	if err := tx.BeginWrite(); err != nil {
		panic(err)
	}

	if tx.catalogWriter == nil {
		tx.Catalog = tx.db.Catalog().Clone()
		// clone the catalog so that it can be modified without affecting the original one.
		tx.catalogWriter = NewCatalogWriter(tx.Catalog)
	}

	return tx.catalogWriter
}
