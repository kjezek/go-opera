package integration

import (
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/go-opera/integration/pebble"
	"github.com/Fantom-foundation/lachesis-base/hash"
	"github.com/Fantom-foundation/lachesis-base/inter/dag"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/utils/cachescale"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

const (
	// metricsGatheringInterval specifies the interval to retrieve leveldb database
	// compaction, io and pause stats to report to the user.
	metricsGatheringInterval = 3 * time.Second
)

type DBProducerWithMetrics struct {
	kvdb.FlushableDBProducer
}

type DropableStoreWithMetrics struct {
	kvdb.DropableStore

	diskReadMeter  metrics.Meter // Meter for measuring the effective amount of data read
	diskWriteMeter metrics.Meter // Meter for measuring the effective amount of data written

	prefixReadGauge [512]metrics.Gauge
	prefixWriteGauge [512]metrics.Gauge
	batchWriteGauge metrics.Gauge

	prefixUpdateDuration [512]int64
	prefixReadDuration [512]int64
	batchWriteDuration int64
	name string

	quitLock sync.Mutex      // Mutex protecting the quit channel access
	quitChan chan chan error // Quit channel to stop the metrics collection before closing the database

	log log.Logger // Contextual logger tracking the database path
}

func WrapDatabaseWithMetrics(db kvdb.FlushableDBProducer) kvdb.FlushableDBProducer {
	wrapper := &DBProducerWithMetrics{db}
	return wrapper
}

func WrapStoreWithMetrics(ds kvdb.DropableStore) *DropableStoreWithMetrics {
	wrapper := &DropableStoreWithMetrics{
		DropableStore: ds,
		quitChan:      make(chan chan error),
	}
	return wrapper
}

func (ds *DropableStoreWithMetrics) Close() error {
	ds.quitLock.Lock()
	defer ds.quitLock.Unlock()

	if ds.quitChan != nil {
		errc := make(chan error)
		ds.quitChan <- errc
		if err := <-errc; err != nil {
			ds.log.Error("Metrics collection failed", "err", err)
		}
		ds.quitChan = nil
	}
	return ds.DropableStore.Close()
}

func prefixIndex(key []byte) int {
	if len(key) == 0 {
		return 0
	} else if key[0] == 'M' {
		return 256 + int(key[1])
	} else {
		return int(key[0])
	}
}

func indexToPrefix(id int) string {
	if id >= 256 {
		id -= 256
		if (id >= 'a' && id <= 'z') || (id >= 'A' && id <= 'Z') {
			return fmt.Sprintf("M%c", id)
		} else {
			return fmt.Sprintf("M%x", id)
		}
	} else {
		if (id >= 'a' && id <= 'z') || (id >= 'A' && id <= 'Z') {
			return fmt.Sprintf("%c", id)
		} else {
			return fmt.Sprintf("%x", id)
		}
	}
}

func (ds *DropableStoreWithMetrics) markPrefix(i int, name string, readVal int64, writeVal int64) {
	if readVal != 0 || writeVal != 0 {
		if ds.prefixReadGauge[i] == nil {
			ds.prefixReadGauge[i] = metrics.GetOrRegisterGauge("opera/chaindata/"+ds.name+"/prefix/read/"+name, nil)
			ds.prefixWriteGauge[i] = metrics.GetOrRegisterGauge("opera/chaindata/"+ds.name+"/prefix/write/"+name, nil)
		}
		ds.prefixReadGauge[i].Update(readVal)
		ds.prefixWriteGauge[i].Update(writeVal)
	}
}

func (ds *DropableStoreWithMetrics) markPrefixSingle(key []byte) (read int64, write int64) {
	i := prefixIndex(key)
	read = atomic.LoadInt64(&ds.prefixReadDuration[i])
	write = atomic.LoadInt64(&ds.prefixUpdateDuration[i])
	ds.markPrefix(i, indexToPrefix(i), read, write)
	return
}

func (ds *DropableStoreWithMetrics) markRange(minI int, maxI int, gaugeI int, name string) (read int64, write int64) {
	for i := minI; i <= maxI; i++ {
		read += atomic.LoadInt64(&ds.prefixReadDuration[i])
		write += atomic.LoadInt64(&ds.prefixUpdateDuration[i])
	}
	ds.markPrefix(gaugeI, name, read, write)
	return
}

func (ds *DropableStoreWithMetrics) markPrefixes() {
	ds.markPrefixSingle([]byte{'e'})
	ds.markRange(256, 511, int('M'), "M") // TrieDatabase + following:
	ds.markPrefixSingle([]byte{'M', 'o'}) // StorageSnaps
	ds.markPrefixSingle([]byte{'M', 'a'}) // AccountSnaps
	ds.markPrefixSingle([]byte{'M', 'c'}) // EvmCodes
	ds.markPrefixSingle([]byte{'L'}) // EvmLogs
	ds.markPrefixSingle([]byte{'g'}) // Genesis
	ds.markPrefixSingle([]byte{'b'}) // Blocks
	ds.markPrefixSingle([]byte{'V'}) // NetVersion
	ds.markPrefixSingle([]byte{'x'}) // TxPositions
	ds.markPrefixSingle([]byte{'P'}) // EpochBlocks
	ds.markPrefixSingle([]byte{'r'}) // Receipts
	ds.markPrefixSingle([]byte{'B'}) // BlockHashes
	ds.markPrefixSingle([]byte{'S'}) // SfcAPI
	ds.markPrefixSingle([]byte{'h'}) // BlockEpochStateHistory
	ds.markPrefixSingle([]byte{'D'}) // BlockEpochState
	ds.markPrefixSingle([]byte{'_'}) // Version
	ds.markPrefixSingle([]byte{'l'}) // HighestLamport
	ds.markPrefixSingle([]byte{'*'}) // LlrLastBlockVotes
	ds.markPrefixSingle([]byte{'!'}) // LlrState
	ds.markPrefixSingle([]byte{'('}) // LlrLastEpochVote
	ds.markPrefixSingle([]byte{'X'}) // Txs
	ds.markPrefixSingle([]byte{'$'}) // LlrBlockVotes
	ds.markPrefixSingle([]byte{'t'}) // JDajc: TransactionTraces
	ds.markRange(0, 511, 511, "all")
	ds.batchWriteGauge.Update(atomic.LoadInt64(&ds.batchWriteDuration))
}

func (ds *DropableStoreWithMetrics) Stat(property string) (string, error) {
	if property == "leveldb.prefixes" {
		out := "Prefixes Put consumed nanoseconds: " + ds.name + "\n"
		out += fmt.Sprintf("BatchWrite: %d\n", atomic.LoadInt64(&ds.batchWriteDuration))
		out += "Prefix\tRead\tWrite\n"
		for i := 0; i < 512; i++ {
			if ds.prefixReadDuration[i] != 0 || ds.prefixUpdateDuration[i] != 0 {
				out += fmt.Sprintf("%s\t%d\t%d\n", indexToPrefix(i), atomic.LoadInt64(&ds.prefixReadDuration[i]), atomic.LoadInt64(&ds.prefixUpdateDuration[i]))
			}
		}
		return out, nil
	} else {
		return ds.DropableStore.Stat(property)
	}
}

func (ds *DropableStoreWithMetrics) Put(key []byte, value []byte) error {
	defer func(start time.Time) { atomic.AddInt64(&ds.prefixUpdateDuration[prefixIndex(key)], int64(time.Since(start))) }(time.Now())
	return ds.DropableStore.Put(key, value)
}

func (ds *DropableStoreWithMetrics) Delete(key []byte) error {
	defer func(start time.Time) { atomic.AddInt64(&ds.prefixUpdateDuration[prefixIndex(key)], int64(time.Since(start))) }(time.Now())
	return ds.DropableStore.Delete(key)
}

func (ds *DropableStoreWithMetrics) Has(key []byte) (bool, error) {
	defer func(start time.Time) { atomic.AddInt64(&ds.prefixReadDuration[prefixIndex(key)], int64(time.Since(start))) }(time.Now())
	return ds.DropableStore.Has(key)
}

func (ds *DropableStoreWithMetrics) Get(key []byte) ([]byte, error) {
	defer func(start time.Time) { atomic.AddInt64(&ds.prefixReadDuration[prefixIndex(key)], int64(time.Since(start))) }(time.Now())
	return ds.DropableStore.Get(key)
}

func (ds *DropableStoreWithMetrics) NewBatch() kvdb.Batch {
	return &batchWithMetrics{ds.DropableStore.NewBatch(), ds}
}

func (ds *DropableStoreWithMetrics) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	defer func(start time.Time) { atomic.AddInt64(&ds.prefixReadDuration[prefixIndex(prefix)], int64(time.Since(start))) }(time.Now())
	return &iteratorWithMetrics{ds.DropableStore.NewIterator(prefix, start), ds, prefix}
}

func (ds *DropableStoreWithMetrics) GetSnapshot() (kvdb.Snapshot, error) {
	s, err := ds.DropableStore.GetSnapshot()
	if err != nil {
		return s, err
	}
	return &snapshotWithMetrics{s, ds}, nil
}

type batchWithMetrics struct {
	kvdb.Batch
	ds *DropableStoreWithMetrics
}

func (b *batchWithMetrics) Put(key []byte, value []byte) error {
	defer func(start time.Time) { atomic.AddInt64(&b.ds.prefixUpdateDuration[prefixIndex(key)], int64(time.Since(start))) }(time.Now())
	return b.Batch.Put(key, value)
}

func (b *batchWithMetrics) Write() error {
	defer func(start time.Time) { atomic.AddInt64(&b.ds.batchWriteDuration, int64(time.Since(start))) }(time.Now())
	return b.Batch.Write()
}

type iteratorWithMetrics struct {
	kvdb.Iterator
	ds *DropableStoreWithMetrics
	prefix []byte
}

func (b *iteratorWithMetrics) Next() bool {
	defer func(start time.Time) { atomic.AddInt64(&b.ds.prefixReadDuration[prefixIndex(b.prefix)], int64(time.Since(start))) }(time.Now())
	return b.Iterator.Next()
}

type snapshotWithMetrics struct {
	kvdb.Snapshot
	ds *DropableStoreWithMetrics
}

func (s *snapshotWithMetrics) Get(key []byte) ([]byte, error) {
	defer func(start time.Time) { atomic.AddInt64(&s.ds.prefixReadDuration[prefixIndex(key)], int64(time.Since(start))) }(time.Now())
	return s.Snapshot.Get(key)
}

func (s *snapshotWithMetrics) NewIterator(prefix []byte, start []byte) kvdb.Iterator {
	defer func(start time.Time) { atomic.AddInt64(&s.ds.prefixReadDuration[prefixIndex(prefix)], int64(time.Since(start))) }(time.Now())
	return &iteratorWithMetrics{s.ds.DropableStore.NewIterator(prefix, start), s.ds, prefix}
}

func (ds *DropableStoreWithMetrics) meter(refresh time.Duration) {
	// Create storage for iostats.
	var iostats [2]float64

	var (
		errc chan error
		merr error
	)

	timer := time.NewTimer(refresh)
	defer timer.Stop()
	// Iterate ad infinitum and collect the stats
	for i := 1; errc == nil && merr == nil; i++ {
		// Retrieve the database iostats.
		ioStats, err := ds.Stat("leveldb.iostats")
		if err != nil {
			ds.log.Error("Failed to read database iostats", "err", err)
			merr = err
			continue
		}
		var nRead, nWrite float64
		parts := strings.Split(ioStats, " ")
		if len(parts) < 2 {
			ds.log.Error("Bad syntax of ioStats", "ioStats", ioStats)
			merr = fmt.Errorf("bad syntax of ioStats %s", ioStats)
			continue
		}
		if n, err := fmt.Sscanf(parts[0], "Read(MB):%f", &nRead); n != 1 || err != nil {
			ds.log.Error("Bad syntax of read entry", "entry", parts[0])
			merr = err
			continue
		}
		if n, err := fmt.Sscanf(parts[1], "Write(MB):%f", &nWrite); n != 1 || err != nil {
			log.Error("Bad syntax of write entry", "entry", parts[1])
			merr = err
			continue
		}
		if ds.diskReadMeter != nil {
			ds.diskReadMeter.Mark(int64((nRead - iostats[0]) * 1024 * 1024))
		}
		if ds.diskWriteMeter != nil {
			ds.diskWriteMeter.Mark(int64((nWrite - iostats[1]) * 1024 * 1024))
		}
		iostats[0], iostats[1] = nRead, nWrite

		ds.markPrefixes()

		// Sleep a bit, then repeat the stats collection
		select {
		case errc = <-ds.quitChan:
			// Quit requesting, stop hammering the database
		case <-timer.C:
			timer.Reset(refresh)
			// Timeout, gather a new set of stats
		}
	}
	if errc == nil {
		errc = <-ds.quitChan
	}
	errc <- merr
}

func (db *DBProducerWithMetrics) OpenDB(name string) (kvdb.DropableStore, error) {
	ds, err := db.FlushableDBProducer.OpenDB(name)
	if err != nil {
		return nil, err
	}
	dm := WrapStoreWithMetrics(ds)
	if strings.HasPrefix(name, "gossip-") || strings.HasPrefix(name, "lachesis-") {
		name = "epochs"
	}
	logger := log.New("database", name)
	dm.log = logger
	dm.diskReadMeter = metrics.GetOrRegisterMeter("opera/chaindata/"+name+"/disk/read", nil)
	dm.diskWriteMeter = metrics.GetOrRegisterMeter("opera/chaindata/"+name+"/disk/write", nil)
	dm.batchWriteGauge = metrics.GetOrRegisterGauge("opera/chaindata/"+name+"/prefix/batchwrite", nil)
	dm.name = name

	// Start up the metrics gathering and return
	go dm.meter(metricsGatheringInterval)
	return dm, nil
}

func DBProducer(chaindataDir string, scale cachescale.Func) kvdb.IterableDBProducer {
	if chaindataDir == "inmemory" || chaindataDir == "" {
		chaindataDir, _ = ioutil.TempDir("", "opera-integration")
	}

	return pebble.NewProducer(chaindataDir)

	/*
	if strings.HasPrefix(chaindataDir, "pebble:") {
		return pebble.NewProducer(chaindataDir[7:])
	}

	if strings.HasPrefix(chaindataDir, "leveldb:") {
		chaindataDir = chaindataDir[8:]
	}
	return leveldb.NewProducer(chaindataDir, func(name string) int {
		return dbCacheSize(name, scale.I)
	})
	 */
}

func CheckDBList(names []string) error {
	if len(names) == 0 {
		return nil
	}
	namesMap := make(map[string]bool)
	for _, name := range names {
		namesMap[name] = true
	}
	if !namesMap["gossip"] {
		return errors.New("gossip DB is not found")
	}
	if !namesMap["lachesis"] {
		return errors.New("lachesis DB is not found")
	}
	return nil
}

func dbCacheSize(name string, scale func(int) int) int {
	if name == "gossip" {
		return scale(128 * opt.MiB)
	}
	if name == "lachesis" {
		return scale(4 * opt.MiB)
	}
	if strings.HasPrefix(name, "lachesis-") {
		return scale(8 * opt.MiB)
	}
	if strings.HasPrefix(name, "gossip-") {
		return scale(8 * opt.MiB)
	}
	return scale(2 * opt.MiB)
}

func dropAllDBs(producer kvdb.IterableDBProducer) {
	names := producer.Names()
	for _, name := range names {
		db, err := producer.OpenDB(name)
		if err != nil {
			continue
		}
		_ = db.Close()
		db.Drop()
	}
}

func dropAllDBsIfInterrupted(rawProducer kvdb.IterableDBProducer) {
	names := rawProducer.Names()
	if len(names) == 0 {
		return
	}
	// if flushID is not written, then previous genesis processing attempt was interrupted
	for _, name := range names {
		db, err := rawProducer.OpenDB(name)
		if err != nil {
			return
		}
		flushID, err := db.Get(FlushIDKey)
		_ = db.Close()
		if flushID != nil || err != nil {
			return
		}
	}
	dropAllDBs(rawProducer)
}

type GossipStoreAdapter struct {
	*gossip.Store
}

func (g *GossipStoreAdapter) GetEvent(id hash.Event) dag.Event {
	e := g.Store.GetEvent(id)
	if e == nil {
		return nil
	}
	return e
}

type DummyFlushableProducer struct {
	kvdb.DBProducer
}

func (p *DummyFlushableProducer) NotFlushedSizeEst() int {
	return 0
}

func (p *DummyFlushableProducer) Flush(_ []byte) error {
	return nil
}
