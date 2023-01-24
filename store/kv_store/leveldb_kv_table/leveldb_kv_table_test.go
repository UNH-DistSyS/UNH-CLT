package leveldb_kv_table

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"testing"

	"github.com/UNH-DistSyS/UNH-CLT/ids"
	"github.com/UNH-DistSyS/UNH-CLT/log"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/readop"
	"github.com/UNH-DistSyS/UNH-CLT/replication/operations/writeop"
	"github.com/UNH-DistSyS/UNH-CLT/store"
	"github.com/UNH-DistSyS/UNH-CLT/store/kv_store/kv_table"
	"github.com/UNH-DistSyS/UNH-CLT/utils"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/syndtr/goleveldb/leveldb"
)

var table kv_table.KVTable
var tempDir string
var tblId uuid.UUID
var underlying_leveldb *leveldb.DB

func setupTableTests(t *testing.T, id ids.ID) {
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
	lvlDbTable := NewLevelDBTable(id, tmpDir, "testtbl", tblId)
	underlying_leveldb = lvlDbTable.leveldb
	table = lvlDbTable
}

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

func TestApplyPayloadOneKey(t *testing.T) {
	setupTableTests(t, *ids.GetIDFromString("1.1"))
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

	// check by reading directly from level db
	valCheck, err := underlying_leveldb.Get(kvitem[0].Key, nil)
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	assert.True(t, utils.AreByteStringsSame(kvitem[0].Value, valCheck))

	kvitem2 := []store.KVItem{{Key: append(pk, []byte("testKey124")...), Value: []byte("")}}

	valCheck, err = underlying_leveldb.Get(kvitem2[0].Key, nil)
	if err == nil {
		assert.Fail(t, "Expect Item Not Found")
		return
	}
}

func TestApplyPayloadOneKeyThenDelete(t *testing.T) {
	setupTableTests(t, *ids.GetIDFromString("1.1"))
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

	// check by reading directly from level db
	valCheck, err := underlying_leveldb.Get(kvitem[0].Key, nil)
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	assert.True(t, utils.AreByteStringsSame(kvitem[0].Value, valCheck))

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

	valCheck, err = underlying_leveldb.Get(kvitem2[0].Key, nil)
	if err == nil {
		assert.Fail(t, "Expect Item Not Found")
		return
	}
}

func TestApplyPayloadWithMultipleKeysThenDelete(t *testing.T) {
	setupTableTests(t, *ids.GetIDFromString("1.1"))
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
		// check by reading directly from level db
		valCheck, err := underlying_leveldb.Get(kvitem[i].Key, nil)
		if err != nil {
			log.Errorf("Level DB error: %v", err)
			assert.Fail(t, "Level DB error", err)
			return
		}
		log.Debugf("Reading %v from leveldb", string(valCheck))
		assert.True(t, utils.AreByteStringsSame(kvitem[i].Value, valCheck))
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
		_, err := underlying_leveldb.Get(kvitem2[i].Key, nil)
		if err == nil {
			log.Errorf("Expect Item %d Not Found", i)
			assert.Fail(t, "Expect Item Not Found")
			return
		}
	}

	valCheck, err := underlying_leveldb.Get(kvitem2[2].Key, nil)
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}
	assert.True(t, utils.AreByteStringsSame(kvitem2[2].Value, valCheck))

	valCheck, err = underlying_leveldb.Get(kvitem[0].Key, nil)
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}
	assert.True(t, utils.AreByteStringsSame(kvitem[0].Value, valCheck))
}

/*****************************************************************
* Read Tests
*****************************************************************/

func TestWriteAndReadOne(t *testing.T) {
	setupTableTests(t, *ids.GetIDFromString("1.1"))
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
	// check by reading directly from level db
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	log.Debugf("read value: %v", string(val[0].Value))
	assert.True(t, utils.AreByteStringsSame(kvitem[0].Value, val[0].Value))
}

func TestReadRange(t *testing.T) {
	setupTableTests(t, *ids.GetIDFromString("1.1"))
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
	// check by reading directly from level db
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	assert.Equal(t, len(kvitem), len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i].Value))
		assert.True(t, utils.AreByteStringsSame(kvitem[i].Value, item.Value))
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
	// check by reading directly from level db
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	assert.Equal(t, len(kvitem)-1, len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i].Value))
		assert.True(t, utils.AreByteStringsSame(kvitem[i].Value, item.Value))
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
	// check by reading directly from level db
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	assert.Equal(t, len(kvitem)-2, len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i+1].Value))
		assert.True(t, utils.AreByteStringsSame(kvitem[i+1].Value, item.Value))
	}
}

func TestReadRange2(t *testing.T) {
	setupTableTests(t, *ids.GetIDFromString("1.1"))
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
	// check by reading directly from level db
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	assert.Equal(t, 3, len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i].Value))
		assert.True(t, utils.AreByteStringsSame(kvitem[i].Value, item.Value))
	}
}

func TestReadAll(t *testing.T) {
	setupTableTests(t, *ids.GetIDFromString("1.1"))
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
	// check by reading directly from level db
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	assert.Equal(t, 6, len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i].Value))
		assert.True(t, utils.AreByteStringsSame(kvitem[i].Value, item.Value))
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
	setupTableTests(t, *ids.GetIDFromString("1.1"))
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
	// check by reading directly from level db
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	assert.Equal(t, len(kvitem), len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem[i].Value))
		//assert.True(t, utils.AreByteStringsSame(kvitem[i].Value, item.Value))
	}
}

func TestReadPrefix(t *testing.T) {
	setupTableTests(t, *ids.GetIDFromString("1.1"))
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
	// check by reading directly from level db
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	assert.Equal(t, len(kvitem1), len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem1[i].Value))
		assert.True(t, utils.AreByteStringsSame(kvitem1[i].Value, item.Value))
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
	// check by reading directly from level db
	if err != nil {
		log.Errorf("Level DB error: %v", err)
		assert.Fail(t, "Level DB error", err)
		return
	}

	assert.Equal(t, len(kvitem2), len(val))

	for i, item := range val {
		log.Debugf("read value: %v, expecting value %v", string(item.Value), string(kvitem2[i].Value))
		assert.True(t, utils.AreByteStringsSame(kvitem2[i].Value, item.Value))
	}
}

func BenchmarkReadPrefix(b *testing.B) {
	log.SetSeverityLevel("info")
	b.SetParallelism(1)
	setupTableTests(nil, *ids.GetIDFromString("1.1"))
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
