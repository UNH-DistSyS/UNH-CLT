package badgerdb_kv_table

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	"github.com/UNH-DistSyS/UNH-CLT/config"
	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/UNH-DistSyS/UNH-CLT/store/kv_store/kv_table"
	"github.com/dgraph-io/badger/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

var underlying_badgerdb *badger.DB
var table kv_table.KVTable
var tempDir string
var tblId uuid.UUID

func cleanupTempDir(t *testing.T) {
	if table != nil {
		table.Close()
	}
	err := os.RemoveAll(tempDir)
	if err != nil {
		if t != nil {
			assert.Fail(t, "failed to cleanup temp dir", err)
		} else {
			log.Errorf("failed to cleanup temp dir")
		}
	}
}

func setupBadgerDBTableTests(t *testing.T, id ids.ID) {
	tmpDir, err := ioutil.TempDir("", "kvdata")
	if err != nil {
		if t != nil {
			assert.Fail(t, "failed creating temp dir")
		} else {
			log.Errorf("failed to create temp dir")
		}
	}
	tempDir = tmpDir
	tblId = uuid.New()
	scfg := config.NewDefaultStoreConfig()
	scfg.DBDir = tmpDir
	badgerTable := NewBadgerDBTable(id, scfg, "testtbl", tblId)
	underlying_badgerdb = badgerTable.badgerdb
	table = badgerTable
}

func TestApplyPayloadOneKeyBadgerDB(t *testing.T) {
	setupBadgerDBTableTests(t, *ids.GetIDFromString("1.1"))
	defer cleanupTempDir(t)

	pk := store.ByteString("testpartition")
	kvitem := []store.KVItem{{Key: append(pk, []byte("testKey123")...), Value: []byte("testVal123")}}
	payload := writeop.WriteOp{
		Items: kvitem,
	}

	err := table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	underlying_badgerdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get(kvitem[0].Key)
		assert.Nil(t, err)

		err = item.Value(func(v []byte) error {
			assert.True(t, bytes.Equal(kvitem[0].Value, v))
			return nil
		})

		assert.Nil(t, err)

		kvitem2 := []store.KVItem{{Key: append(pk, []byte("testKey124")...), Value: []byte("")}}
		item, err = txn.Get(kvitem2[0].Key)
		if err != nil {
			assert.Equal(t, badger.ErrKeyNotFound, err)
		} else {
			assert.Fail(t, "expect key not found")
		}

		return nil
	})
}

func TestApplyPayloadOneKeyThenDelete(t *testing.T) {
	setupBadgerDBTableTests(t, *ids.GetIDFromString("1.1"))
	defer cleanupTempDir(t)

	pk := store.ByteString("testpartition")
	kvitem := []store.KVItem{{Key: append(pk, []byte("testKey123")...), Value: []byte("testVal123")}}
	payload := writeop.WriteOp{
		Items: kvitem,
	}

	err := table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	// check by reading directly from badgerdb
	underlying_badgerdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get(kvitem[0].Key)
		assert.Nil(t, err)

		err = item.Value(func(v []byte) error {
			assert.True(t, bytes.Equal(kvitem[0].Value, v))
			return nil
		})

		assert.Nil(t, err)
		return nil
	})

	kvitem2 := []store.KVItem{{Key: append(pk, []byte("testKey123")...), Value: nil}}
	payload = writeop.WriteOp{
		Items: kvitem2,
	}

	err = table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	// check by reading directly from badgerdb
	underlying_badgerdb.View(func(txn *badger.Txn) error {
		_, err := txn.Get(kvitem[0].Key)
		assert.Equal(t, badger.ErrKeyNotFound, err)
		return nil
	})
}

func TestApplyPayloadWithMultipleKeysThenDelete(t *testing.T) {
	setupBadgerDBTableTests(t, *ids.GetIDFromString("1.1"))
	defer cleanupTempDir(t)

	pk := store.ByteString("testpartition")
	kvitem := []store.KVItem{
		{Key: append(pk, []byte("testKey123")...), Value: []byte("testVal123")},
		{Key: append(pk, []byte("testKey124")...), Value: []byte("testVal124")},
		{Key: append(pk, []byte("testKey125")...), Value: []byte("testVal125")},
	}
	payload := writeop.WriteOp{
		Items: kvitem,
	}

	err := table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	for i := 0; i < len(kvitem); i++ {
		// check by reading directly from badgerdb
		underlying_badgerdb.View(func(txn *badger.Txn) error {
			item, err := txn.Get(kvitem[i].Key)
			assert.Nil(t, err)

			err = item.Value(func(v []byte) error {
				assert.True(t, bytes.Equal(kvitem[i].Value, v))
				return nil
			})

			assert.Nil(t, err)
			return nil
		})
	}

	kvitem2 := []store.KVItem{
		{Key: append(pk, []byte("testKey124")...), Value: nil},
		{Key: append(pk, []byte("testKey125")...), Value: nil},
		{Key: append(pk, []byte("testKey126")...), Value: []byte("testVal126")},
	}
	payload = writeop.WriteOp{
		Items: kvitem2,
	}

	err = table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}
	for i := 0; i < len(kvitem2)-1; i++ {
		underlying_badgerdb.View(func(txn *badger.Txn) error {
			_, err := txn.Get(kvitem2[i].Key)
			assert.Equal(t, badger.ErrKeyNotFound, err)
			return nil
		})
	}

	underlying_badgerdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get(kvitem2[2].Key)
		assert.Nil(t, err)

		err = item.Value(func(v []byte) error {
			assert.True(t, bytes.Equal(kvitem2[2].Value, v))
			return nil
		})

		assert.Nil(t, err)
		return nil
	})

	underlying_badgerdb.View(func(txn *badger.Txn) error {
		item, err := txn.Get(kvitem[0].Key)
		assert.Nil(t, err)

		err = item.Value(func(v []byte) error {
			assert.True(t, bytes.Equal(kvitem[0].Value, v))
			return nil
		})

		assert.Nil(t, err)
		return nil
	})
}

/*****************************************************************
* Read Tests
*****************************************************************/

func TestWriteAndReadOne(t *testing.T) {
	setupBadgerDBTableTests(t, *ids.GetIDFromString("1.1"))
	defer cleanupTempDir(t)

	pk := store.ByteString("testpartition")
	kvitem := []store.KVItem{{Key: append(pk, []byte("testKey123")...), Value: []byte("testVal123")}}
	payload := writeop.WriteOp{
		Items: kvitem,
	}

	err := table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	readOp := &readop.ReadOp{
		TableUUID: tblId,
		ReadMode:  readop.POINT,
		SkipFirst: false,
		SkipLast:  false,
		StartKey:  append(pk, []byte("testKey123")...),
		EndKey:    nil,
	}

	val, err := table.Read(readOp)
	if err != nil {
		log.Errorf("BadgerDB error: %v", err)
		assert.Fail(t, "BadgerDB error", err)
		return
	}

	log.Debugf("read value: %v", string(val[0].Value))
	assert.True(t, bytes.Equal(kvitem[0].Value, val[0].Value))
	assert.True(t, bytes.Equal(kvitem[0].Key, val[0].Key))
}

func TestReadRange(t *testing.T) {
	setupBadgerDBTableTests(t, *ids.GetIDFromString("1.1"))
	defer cleanupTempDir(t)

	pk := store.ByteString("testpartition")
	kvitem := []store.KVItem{
		{Key: append(pk, []byte("testKey123")...), Value: []byte("testVal123")},
		{Key: append(pk, []byte("testKey124")...), Value: []byte("testVal124")},
		{Key: append(pk, []byte("testKey125")...), Value: []byte("testVal125")},
		{Key: append(pk, []byte("testKey127")...), Value: []byte("testVal127")},
	}
	payload := writeop.WriteOp{
		Items: kvitem,
	}

	err := table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	readOp := &readop.ReadOp{
		TableUUID: tblId,
		ReadMode:  readop.RANGE,
		SkipFirst: false,
		SkipLast:  false,
		StartKey:  append(pk, []byte("testKey123")...),
		EndKey:    append(pk, []byte("testKey127")...),
	}

	val, err := table.Read(readOp)
	if err != nil {
		log.Errorf("BadgerDB error: %v", err)
		assert.Fail(t, "BadgerDB error", err)
		return
	}

	assert.Equal(t, len(kvitem), len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i].Value))
		assert.True(t, bytes.Equal(kvitem[i].Value, item.Value))
		assert.True(t, bytes.Equal(kvitem[i].Key, item.Key))
	}

	readOp = &readop.ReadOp{
		TableUUID: tblId,
		ReadMode:  readop.RANGE,
		SkipFirst: false,
		SkipLast:  true,
		StartKey:  append(pk, []byte("testKey123")...),
		EndKey:    append(pk, []byte("testKey127")...),
	}

	val, err = table.Read(readOp)
	if err != nil {
		log.Errorf("BadgerDB error: %v", err)
		assert.Fail(t, "BadgerDB error", err)
		return
	}

	assert.Equal(t, len(kvitem)-1, len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i].Value))
		assert.True(t, bytes.Equal(kvitem[i].Value, item.Value))
		assert.True(t, bytes.Equal(kvitem[i].Key, item.Key))
	}

	readOp = &readop.ReadOp{
		TableUUID: tblId,
		ReadMode:  readop.RANGE,
		SkipFirst: true,
		SkipLast:  true,
		StartKey:  append(pk, []byte("testKey123")...),
		EndKey:    append(pk, []byte("testKey127")...),
	}

	val, err = table.Read(readOp)
	if err != nil {
		log.Errorf("BadgerDB error: %v", err)
		assert.Fail(t, "BadgerDB error", err)
		return
	}

	assert.Equal(t, len(kvitem)-2, len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i+1].Value))
		assert.True(t, bytes.Equal(kvitem[i+1].Value, item.Value))
		assert.True(t, bytes.Equal(kvitem[i+1].Key, item.Key))
	}
}

func TestReadRange2(t *testing.T) {
	setupBadgerDBTableTests(t, *ids.GetIDFromString("1.1"))
	defer cleanupTempDir(t)

	pk := store.ByteString("testpartition")
	kvitem := []store.KVItem{
		{Key: append(pk, []byte("testKey123")...), Value: []byte("testVal123")},
		{Key: append(pk, []byte("testKey124")...), Value: []byte("testVal124")},
		{Key: append(pk, []byte("testKey125")...), Value: []byte("testVal125")},
		{Key: append(pk, []byte("testKey131")...), Value: []byte("testVal131")},
		{Key: append(pk, []byte("testKey132")...), Value: []byte("testVal132")},
		{Key: append(pk, []byte("testKey133")...), Value: []byte("testVal133")},
	}
	payload := writeop.WriteOp{
		Items: kvitem,
	}

	err := table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	readOp := &readop.ReadOp{
		TableUUID: tblId,
		ReadMode:  readop.RANGE,
		SkipFirst: false,
		SkipLast:  false,
		StartKey:  append(pk, []byte("testKey12")...),
		EndKey:    append(pk, []byte("testKey13")...),
	}

	val, err := table.Read(readOp)
	if err != nil {
		log.Errorf("BadgerDB error: %v", err)
		assert.Fail(t, "BadgerDB error", err)
		return
	}

	assert.Equal(t, 3, len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i].Value))
		assert.True(t, bytes.Equal(kvitem[i].Value, item.Value))
		assert.True(t, bytes.Equal(kvitem[i].Key, item.Key))
	}
}

func TestReadAll(t *testing.T) {
	setupBadgerDBTableTests(t, *ids.GetIDFromString("1.1"))
	defer cleanupTempDir(t)

	pk := store.ByteString("testpartition")
	kvitem := []store.KVItem{
		{Key: append(pk, []byte("testKey123")...), Value: []byte("testVal123")},
		{Key: append(pk, []byte("testKey124")...), Value: []byte("testVal124")},
		{Key: append(pk, []byte("testKey125")...), Value: []byte("testVal125")},
		{Key: append(pk, []byte("testKey131")...), Value: []byte("testVal131")},
		{Key: append(pk, []byte("testKey132")...), Value: []byte("testVal132")},
		{Key: append(pk, []byte("testKey133")...), Value: []byte("testVal133")},
	}
	payload := writeop.WriteOp{
		Items: kvitem,
	}

	err := table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	readOp := &readop.ReadOp{
		TableUUID: tblId,
		ReadMode:  readop.PREFIX,
		SkipFirst: false,
		SkipLast:  false,
		StartKey:  nil,
		EndKey:    nil,
	}

	val, err := table.Read(readOp)
	if err != nil {
		log.Errorf("BadgerDB error: %v", err)
		assert.Fail(t, "BadgerDB error", err)
		return
	}

	assert.Equal(t, 6, len(val))

	for i, item := range val {
		assert.True(t, bytes.Equal(kvitem[i].Value, item.Value))
		assert.True(t, bytes.Equal(kvitem[i].Key, item.Key))
	}
}

func intToBytes(n int32) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, n)
	if err != nil {
		log.Debugf("binary.Write failed: %v", err)
	}
	log.Debugf("%d -> %v", n, buf.Bytes())
	return buf.Bytes()
}

func TestReadRangeInt(t *testing.T) {
	setupBadgerDBTableTests(t, *ids.GetIDFromString("1.1"))
	defer cleanupTempDir(t)

	//pk := store.ByteString("testpartition")
	kvitem := []store.KVItem{
		{Key: intToBytes(114), Value: []byte("testVal114")},
		{Key: intToBytes(115), Value: []byte("testVal115")},
		{Key: intToBytes(116), Value: []byte("testVal116")},
		{Key: intToBytes(117), Value: []byte("testVal117")},
		{Key: intToBytes(200), Value: []byte("testVal200")},
		{Key: intToBytes(201), Value: []byte("testVal201")},
		{Key: intToBytes(280), Value: []byte("testVal280")},
		{Key: intToBytes(281), Value: []byte("testVal281")},
		{Key: intToBytes(282), Value: []byte("testVal282")},
	}
	payload := writeop.WriteOp{
		Items: kvitem,
	}

	err := table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	readOp := &readop.ReadOp{
		TableUUID: tblId,
		ReadMode:  readop.RANGE,
		SkipFirst: false,
		SkipLast:  false,
		StartKey:  intToBytes(114),
		EndKey:    intToBytes(282),
	}

	val, err := table.Read(readOp)
	if err != nil {
		log.Errorf("BadgerDB error: %v", err)
		assert.Fail(t, "BadgerDB error", err)
		return
	}

	assert.Equal(t, len(kvitem), len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i].Value))
		assert.True(t, bytes.Equal(kvitem[i].Value, item.Value))
		assert.True(t, bytes.Equal(kvitem[i].Key, item.Key))
	}
}

func TestReadPrefix(t *testing.T) {
	setupBadgerDBTableTests(t, *ids.GetIDFromString("1.1"))
	defer cleanupTempDir(t)

	pk := store.ByteString("testpartition1")
	kvitem1 := []store.KVItem{
		{Key: append(pk, []byte("testKey123")...), Value: []byte("testVal123")},
		{Key: append(pk, []byte("testKey124")...), Value: []byte("testVal124")},
		{Key: append(pk, []byte("testKey125")...), Value: []byte("testVal125")},
		{Key: append(pk, []byte("testKey127")...), Value: []byte("testVal127")},
	}
	payload := writeop.WriteOp{
		Items: kvitem1,
	}

	err := table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	pk2 := store.ByteString("testpartition2")
	kvitem2 := []store.KVItem{
		{Key: append(pk2, []byte("testKey123")...), Value: []byte("testVal123-2")},
		{Key: append(pk2, []byte("testKey127")...), Value: []byte("testVal127-2")},
	}
	payload2 := writeop.WriteOp{
		Items: kvitem2,
	}

	err = table.Write(&payload2)
	if err != nil {
		log.Errorf("Table error: %v", err)
		assert.Fail(t, "Table error", err)
		return
	}

	readOp := &readop.ReadOp{
		TableUUID: tblId,
		ReadMode:  readop.PREFIX,
		SkipFirst: false,
		SkipLast:  false,
		StartKey:  pk,
		EndKey:    nil,
	}

	val, err := table.Read(readOp)
	if err != nil {
		log.Errorf("BadgerDB error: %v", err)
		assert.Fail(t, "BadgerDB error", err)
		return
	}

	assert.Equal(t, len(kvitem1), len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem1[i].Value))
		assert.True(t, bytes.Equal(kvitem1[i].Value, item.Value))
		assert.True(t, bytes.Equal(kvitem1[i].Key, item.Key))
	}

	readOp = &readop.ReadOp{
		TableUUID: tblId,
		ReadMode:  readop.PREFIX,
		SkipFirst: false,
		SkipLast:  false,
		StartKey:  pk2,
		EndKey:    nil,
	}

	val, err = table.Read(readOp)
	if err != nil {
		log.Errorf("Badger DB error: %v", err)
		assert.Fail(t, "Badger DB error", err)
		return
	}

	assert.Equal(t, len(kvitem2), len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem2[i].Value))
		assert.True(t, bytes.Equal(kvitem2[i].Value, item.Value))
		assert.True(t, bytes.Equal(kvitem2[i].Key, item.Key))
	}
}

func BenchmarkReadPrefix(b *testing.B) {
	log.SetSeverityLevel("info")
	b.SetParallelism(1)
	setupBadgerDBTableTests(nil, *ids.GetIDFromString("1.1"))
	defer cleanupTempDir(nil)

	pk := store.ByteString("testpartition1")
	kvitem1 := []store.KVItem{
		{Key: append(pk, []byte("testKey123")...), Value: []byte("testVal123")},
		{Key: append(pk, []byte("testKey124")...), Value: []byte("testVal124")},
		{Key: append(pk, []byte("testKey125")...), Value: []byte("testVal125")},
		{Key: append(pk, []byte("testKey127")...), Value: []byte("testVal127")},
	}
	payload := writeop.WriteOp{
		Items: kvitem1,
	}

	err := table.Write(&payload)
	if err != nil {
		log.Errorf("Table error: %v", err)
		return
	}

	pk2 := store.ByteString("testpartition2")
	kvitem2 := []store.KVItem{
		{Key: append(pk2, []byte("testKey123")...), Value: []byte("testVal123-2")},
		{Key: append(pk2, []byte("testKey127")...), Value: []byte("testVal127-2")},
	}
	payload2 := writeop.WriteOp{
		Items: kvitem2,
	}

	err = table.Write(&payload2)
	if err != nil {
		log.Errorf("Table error: %v", err)
		return
	}

	readOp := &readop.ReadOp{
		TableUUID: tblId,
		ReadMode:  readop.PREFIX,
		SkipFirst: false,
		SkipLast:  false,
		StartKey:  pk,
		EndKey:    nil,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = table.Read(readOp)
	}

}
