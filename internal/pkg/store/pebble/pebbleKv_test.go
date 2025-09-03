package pebble

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/pebble/pebble_kv"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/test/common"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
)

const (
	PebbleKvDBPath = "test_pebble"
	PebbleKvTable  = "test"
)

func TestPebbleGetSetDelete(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	require.NoError(t, ks.Set("pk1", "pv1"))

	var val string
	ok, err := ks.Get("pk1", &val)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, "pv1", val)

	require.NoError(t, ks.Set("pk1", "pv2"))
	ok, err = ks.Get("pk1", &val)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, "pv2", val)

	require.NoError(t, ks.Delete("pk1"))
	ok, err = ks.Get("pk1", &val)
	require.False(t, ok)
	require.NoError(t, err)

	require.NoError(t, ks.Set("pk2", "pv2"))
	ok, err = ks.Get("pk2", &val)
	require.True(t, ok)
	require.NoError(t, err)
	require.Equal(t, "pv2", val)

	require.NoError(t, ks.Delete("pk2"))
	ok, err = ks.Get("pk2", &val)
	require.False(t, ok)
	require.NoError(t, err)
}

func TestPebbleKvSetnx(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvSetnx(ks, t)
}

func TestPebbleKvSet(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvSet(ks, t)
}

func TestPebbleKvGet(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvGet(ks, t)
}

func TestPebbleKvSetGet(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvSetGet(ks, t)
}

func TestPebbleKvKeys(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvKeys(10, ks, t)
}

func TestPebbleKvAll(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvAll(10, ks, t)
}

func TestPebbleKvGetKeyedState(t *testing.T) {
	ks, db, abs := setupPebbleKv()
	defer cleanPebbleKv(db, abs)

	common.TestKvGetKeyedState(ks, t)
}

func setupPebbleKv() (kv.KeyValue, definition.Database, string) {
	absPath, err := filepath.Abs("test_pebble")
	if err != nil {
		panic(err)
	}

	err = deletePebbleIfExists(absPath)
	if err != nil {
		panic(err)
	}

	config := definition.Config{
		Type: "pebble",
		Pebble: definition.PebbleConfig{
			Path: absPath,
			Name: PebbleKvDBPath,
		},
	}

	db, err := pebble.NewPebbleDatabase(config, PebbleKvDBPath)
	if err != nil {
		panic(err)
	}

	err = db.Connect()
	if err != nil {
		panic(err)
	}

	builder := NewStoreBuilder(db.(KVDatabase))
	store, err := builder.CreateStore(PebbleKvTable)
	if err != nil {
		panic(err)
	}

	return store, db, absPath
}

func deletePebbleIfExists(path string) error {
	return os.RemoveAll(path)
}

func cleanPebbleKv(db definition.Database, abs string) {
	_ = db.Disconnect()
	_ = deletePebbleIfExists(abs)
}
