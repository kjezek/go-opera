package badger

import (
	"errors"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/dgraph-io/badger/v3"
)

// NewBatch creates a write-only key-value store that buffers changes to its host
// database until a final write is called.
func (db *Database) NewBatch() kvdb.Batch {
	return &batch{
		db: db.db,
		txn: nil,
		ops: make([]batchEntry, 0, 100),
		size: 0,
	}
}

// batch is a write-only batch that commits changes to its host database
// when Write is called. A batch cannot be used concurrently.
type batch struct {
	db   *badger.DB
	txn  *badger.Txn
	ops  []batchEntry
	size int
}

type batchEntry struct {
	Key       []byte
	Value     []byte
}

// Put inserts the given value into the batch for later committing.
func (b *batch) Put(key, value []byte) error {
	if value == nil {
		return errors.New("unable to put nil value into badger")
	}
	if b.txn == nil {
		b.txn = b.db.NewTransaction(true)
	}
	err := b.txn.Set(key, value)
	if err != nil {
		return err
	}
	entry := batchEntry{
		Key:   key,
		Value: value,
	}
	b.ops = append(b.ops, entry)
	b.size += len(value)
	return nil
}

// Delete inserts the key removal into the batch for later committing.
func (b *batch) Delete(key []byte) error {
	if b.txn == nil {
		b.txn = b.db.NewTransaction(true)
	}
	err := b.txn.Delete(key)
	if err != nil {
		return err
	}
	entry := batchEntry{
		Key:   key,
		Value: nil,
	}
	b.ops = append(b.ops, entry)
	b.size++
	return err
}

// ValueSize retrieves the amount of data queued up for writing.
func (b *batch) ValueSize() int {
	return b.size
}

// Write flushes any accumulated data to disk.
func (b *batch) Write() error {
	if b.txn != nil {
		return b.txn.Commit()
	}
	return nil
}

// Reset resets the batch for reuse.
func (b *batch) Reset() {
	if b.txn != nil {
		b.txn.Discard()
		b.txn = nil
		b.ops = make([]batchEntry, 0, 100)
		b.size = 0
	}
}

// Replay replays the batch contents.
func (b *batch) Replay(w kvdb.Writer) (err error) {
	for _, entry := range b.ops {
		if entry.Value == nil {
			err = w.Delete(entry.Key)
		} else {
			err = w.Put(entry.Key, entry.Value)
		}
		if err != nil {
			break
		}
	}
	return
}
