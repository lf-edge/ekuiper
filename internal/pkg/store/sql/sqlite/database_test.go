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

package sqlite

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/pkg/store/definition"
)

func TestSqliteDatabaseConnectAndCorrupt(t *testing.T) {
	tempDir := t.TempDir()

	c := definition.Config{
		Sqlite: definition.SqliteConfig{
			Path: tempDir,
		},
	}

	// 1. Normal creation and connect
	db, err := NewSqliteDatabase(c, "test.db")
	require.NoError(t, err)

	err = db.Connect()
	require.NoError(t, err)

	err = db.Disconnect()
	require.NoError(t, err)

	// 2. Corrupt the database file
	dbPath := filepath.Join(tempDir, "test.db")
	err = os.WriteFile(dbPath, []byte("this is corrupt sqlite data that is not a valid sqlite db"), 0o600)
	require.NoError(t, err)

	// 3. Connect to corrupted DB must fail
	corruptDb, err := NewSqliteDatabase(c, "test.db")
	require.NoError(t, err)

	err = corruptDb.Connect()
	require.Error(t, err)
}

func TestSqliteDatabaseCustomName(t *testing.T) {
	tempDir := t.TempDir()

	c := definition.Config{
		Sqlite: definition.SqliteConfig{
			Path: tempDir,
			Name: "custom.db",
		},
	}

	db, err := NewSqliteDatabase(c, "sqliteKV.db")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(tempDir, "custom.db"), db.(*Database).Path)

	// Custom name also applies to auxiliary database names in legacy behavior
	dbAux, err := NewSqliteDatabase(c, "checkpoint.db")
	require.NoError(t, err)
	require.Equal(t, filepath.Join(tempDir, "custom.db"), dbAux.(*Database).Path)
}
