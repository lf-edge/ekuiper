// Copyright 2024 EMQ Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
)

func TestSetup_AuxStoreBestEffortAndNoFallback(t *testing.T) {
	tempDir := t.TempDir()

	c := definition.Config{
		Type:         "sqlite",
		ExtStateType: "sqlite",
		Sqlite: definition.SqliteConfig{
			Path: tempDir,
		},
	}

	// 1. Initial healthy setup
	err := Setup(c, true)
	require.NoError(t, err)
	DisconnectAll()

	// 2. Corrupt auxiliary database checkpoint.db
	auxPath := filepath.Join(tempDir, "checkpoint.db")
	err = os.WriteFile(auxPath, []byte("corrupted auxiliary db data"), 0o600)
	require.NoError(t, err)

	// 3. Setup again - auxiliary store corruption must NOT block Setup (best-effort)
	err = Setup(c, true)
	require.NoError(t, err, "auxiliary store failure must not abort Setup in SQLite")

	// Verify corrupted file was NOT automatically deleted
	fi, statErr := os.Stat(auxPath)
	require.NoError(t, statErr)
	require.Equal(t, int64(len("corrupted auxiliary db data")), fi.Size())

	// Verify GetTS and DropTS return error and DO NOT fall back to global stores
	_, tsErr := GetTS("test_rule")
	require.Error(t, tsErr)
	require.Contains(t, tsErr.Error(), "checkpoint store initialization failed")

	dropErr := DropTS("test_rule")
	require.Error(t, dropErr)
	require.Contains(t, dropErr.Error(), "checkpoint store initialization failed")

	// Normal KV store on global store continues to work
	kv, err := GetKV("normal_kv")
	require.NoError(t, err)
	require.NoError(t, kv.Set("key1", "val1"))
	var val string
	ok, err := kv.Get("key1", &val)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "val1", val)

	DisconnectAll()
}

func TestSetup_NonSqliteAuxStoreFailsStrictly(t *testing.T) {
	// For non-sqlite store type, Setup does not ignore aux store errors
	c := definition.Config{
		Type: "unknown_backend",
	}
	err := Setup(c, false)
	require.Error(t, err)
}

func TestSetup_ExtStateNonSqliteFailsStrictly(t *testing.T) {
	tempDir := t.TempDir()
	c := definition.Config{
		Type:         "sqlite",
		ExtStateType: "unknown_backend",
		Sqlite: definition.SqliteConfig{
			Path: tempDir,
		},
	}
	err := Setup(c, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown extStateStore type")
	var mainErr *MainStoreError
	require.False(t, errors.As(err, &mainErr), "auxiliary extState failure must not be classified as MainStoreError")
	DisconnectAll()
}

func TestSetup_MainStoreFailureReturnsMainStoreError(t *testing.T) {
	tempDir := t.TempDir()
	mainPath := filepath.Join(tempDir, "sqliteKV.db")
	require.NoError(t, os.WriteFile(mainPath, []byte("corrupted main db"), 0o600))

	c := definition.Config{
		Type:         "sqlite",
		ExtStateType: "sqlite",
		Sqlite: definition.SqliteConfig{
			Path: tempDir,
		},
	}
	err := Setup(c, false)
	require.Error(t, err)
	var mainErr *MainStoreError
	require.True(t, errors.As(err, &mainErr), "main store failure must return MainStoreError")
	DisconnectAll()
}

func TestSetup_TraceDbFailureDoesNotAbortStartup(t *testing.T) {
	tempDir := t.TempDir()
	tracePath := filepath.Join(tempDir, "trace.db")
	require.NoError(t, os.WriteFile(tracePath, []byte("corrupted trace db"), 0o600))

	c := definition.Config{
		Type:         "sqlite",
		ExtStateType: "sqlite",
		Sqlite: definition.SqliteConfig{
			Path: tempDir,
		},
	}
	err := Setup(c, false)
	require.NoError(t, err, "trace.db failure must be best-effort and not block startup")
	require.Nil(t, TraceStores)
	DisconnectAll()
}
