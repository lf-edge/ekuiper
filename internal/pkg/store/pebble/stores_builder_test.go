package pebble

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
)

func TestBuildStores_Success(t *testing.T) {
	dir := t.TempDir()
	cfg := definition.Config{Type: "pebble", Pebble: definition.PebbleConfig{Path: dir, Name: "builder_ok"}}
	kvb, tsb, err := BuildStores(cfg, "builder_ok")
	require.NoError(t, err)
	require.NotNil(t, kvb)
	require.NotNil(t, tsb)
	if sb, ok := kvb.(*StoreBuilder); ok {
		if dc, ok := any(sb.database).(interface{ Disconnect() error }); ok {
			t.Cleanup(func() { _ = dc.Disconnect() })
		}
	}
}

func TestBuildPebbleStore_Success(t *testing.T) {
	dir := t.TempDir()
	cfg := definition.Config{Type: "pebble", Pebble: definition.PebbleConfig{Path: dir, Name: "only_store"}}
	db, err := BuildPebbleStore(cfg, "only_store")
	require.NoError(t, err)
	require.NotNil(t, db)
	if dc, ok := any(db).(interface{ Disconnect() error }); ok {
		t.Cleanup(func() { _ = dc.Disconnect() })
	}
}

func TestBuildStores_Fail_EmptyPath(t *testing.T) {
	cfg := definition.Config{Type: "pebble", Pebble: definition.PebbleConfig{Path: "", Name: "bad"}}
	_, _, err := BuildStores(cfg, "bad")
	require.Error(t, err)
}

func TestBuildPebbleStore_Fail_EmptyPath(t *testing.T) {
	cfg := definition.Config{Type: "pebble", Pebble: definition.PebbleConfig{Path: "", Name: "bad"}}
	_, err := BuildPebbleStore(cfg, "bad")
	require.Error(t, err)
}

func TestBuildStores_RecognizedType(t *testing.T) {
	dir := t.TempDir()
	abs, _ := filepath.Abs(dir)
	cfg := definition.Config{Type: "pebble", Pebble: definition.PebbleConfig{Path: abs, Name: "recog"}}
	kvb, tsb, err := BuildStores(cfg, "recog")
	require.NoError(t, err)
	require.NotNil(t, kvb)
	require.NotNil(t, tsb)
	if sb, ok := kvb.(*StoreBuilder); ok {
		if dc, ok := any(sb.database).(interface{ Disconnect() error }); ok {
			t.Cleanup(func() { _ = dc.Disconnect() })
		}
	}
}
