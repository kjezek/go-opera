package badger

import (
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/dgraph-io/badger/v3"
	"sync"
)

// Database is a persistent key-value store. Apart from basic data storage
// functionality it also supports batch writes and iterating over the keyspace in
// binary-alphabetical order.
type Database struct {
	fn string     // filename for reporting
	db *badger.DB // LevelDB instance

	quitLock sync.Mutex // Mutex protecting the quit channel access

	onClose func() error
	onDrop  func()
}

// New returns a wrapped LevelDB object. The namespace is the prefix that the
// metrics reporting should use for surfacing internal stats.
func New(path string, close func() error, drop func()) (*Database, error) {
	db, err := badger.Open(badger.DefaultOptions(path))

	if err != nil {
		return nil, err
	}
	// Assemble the wrapper with all the registered metrics
	ldb := Database{
		fn: path,
		db: db,
		onClose: close,
		onDrop: drop,
	}
	return &ldb, nil
}

// Close stops the metrics collection, flushes any pending data to disk and closes
// all io accesses to the underlying key-value store.
func (db *Database) Close() error {
	db.quitLock.Lock()
	defer db.quitLock.Unlock()

	if db.db == nil {
		panic("already closed")
	}

	ldb := db.db
	db.db = nil

	if db.onClose != nil {
		if err := db.onClose(); err != nil {
			return err
		}
		db.onClose = nil
	}
	if err := ldb.Close(); err != nil {
		return err
	}
	return nil
}

// Drop whole database.
func (db *Database) Drop() {
	if db.db != nil {
		panic("Close database first!")
	}
	if db.onDrop != nil {
		db.onDrop()
	}
}

// Has retrieves if a key is present in the key-value store.
func (db *Database) Has(key []byte) (bool, error) {
	err := db.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		return err
	})
	if err == badger.ErrKeyNotFound {
		return false, nil
	}
	if err == nil {
		return true, nil
	}
	return false, err
}

// Get retrieves the given key if it's present in the key-value store.
func (db *Database) Get(key []byte) (out []byte, err error) {
	err = db.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err == nil {
			out, err = item.ValueCopy([]byte{})
		}
		return err
	})
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return out, err
}

// Put inserts the given value into the key-value store.
func (db *Database) Put(key []byte, value []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Delete removes the key from the key-value store.
func (db *Database) Delete(key []byte) error {
	return db.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// NewIterator creates a binary-alphabetical iterator over a subset
// of database content with a particular key prefix, starting at a particular
// initial key (or after, if it does not exist).
func (db *Database) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	if prefix != nil && start != nil {
		start = append(prefix, start...)
	}
	txn := db.db.NewTransaction(false)
	x := iterator{
		txn.NewIterator(badger.IteratorOptions{Prefix: prefix}),
		false,
		false,
		start,
		txn,
		nil,
	}
	return &x
}

type iterator struct {
	*badger.Iterator
	isStarted bool
	isClosed bool
	start []byte
	txn *badger.Txn
	err error
}

func (it *iterator) Next() bool {
	if it.isStarted {
		it.Iterator.Next()
	} else {
		if it.start != nil {
			it.Iterator.Seek(it.start)
		} else {
			it.Iterator.Rewind()
		}
		it.isStarted = true
	}
	return it.Valid()
}

func (it *iterator) Error() error {
	return it.err
}

func (it *iterator) Key() []byte {
	return it.Iterator.Item().KeyCopy([]byte{})
}

func (it *iterator) Value() (out []byte) {
	out, it.err = it.Iterator.Item().ValueCopy([]byte{})
	return
}

func (it *iterator) Release() {
	if it.isClosed {
		return
	}
	it.Iterator.Close() // must not be called multiple times
	it.txn.Discard()
	it.isClosed = true
}

// Stat returns a particular internal stat of the database.
func (db *Database) Stat(property string) (string, error) {
	return "", badger.ErrKeyNotFound
}

// Compact flattens the underlying data store for the given key range. In essence,
// deleted and overwritten versions are discarded, and the data is rearranged to
// reduce the cost of operations needed to access them.
//
// A nil start is treated as a key before all keys in the data store; a nil limit
// is treated as a key after all keys in the data store. If both is nil then it
// will compact entire data store.
func (db *Database) Compact(start []byte, limit []byte) error {
	// badger does not support compact of a range, however it is used only to run the compaction part-by-part
	// in for cycle, so we can run in only for the first iteration instead
	if start[0] == byte(0) {
		return db.db.Flatten(1)
	}
	return nil
}

// Path returns the path to the database directory.
func (db *Database) Path() string {
	return db.fn
}
