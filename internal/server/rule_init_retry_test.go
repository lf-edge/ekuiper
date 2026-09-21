// Copyright 2025 EMQ Technologies Co., Ltd.
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

package server

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/pkg/ast"
)

func TestInitFromLocKeepsMarkerUntilSuccessful(t *testing.T) {
	const name = "sf1024_retry_source"
	t.Cleanup(func() { _, _ = streamProcessor.DropStream(name, ast.TypeStream) })
	loc := t.TempDir()
	initFile := filepath.Join(loc, "init.json")
	require.NoError(t, os.WriteFile(initFile, []byte(`{"streams":{"sf1024_bad_sql":"not SQL"},"tables":{"sf1024_unavailable_lookup":"CREATE TABLE sf1024_unavailable_lookup () WITH (DATASOURCE=\"demo\", TYPE=\"missing_source\", FORMAT=\"JSON\", KEY=\"id\", KIND=\"lookup\")"}}`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(loc, "initialized123"), nil, 0o644))
	require.NoError(t, initFromLoc(loc))
	require.EqualValues(t, 123, findInitializedTime(loc))
	require.NoError(t, os.WriteFile(initFile, []byte(`{"streams":{"sf1024_retry_source":"CREATE STREAM sf1024_retry_source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}}`), 0o644))
	require.NoError(t, initFromLoc(loc))
	info, err := os.Stat(initFile)
	require.NoError(t, err)
	require.Equal(t, info.ModTime().UnixMilli(), findInitializedTime(loc))
	_, err = streamProcessor.GetStream(name, ast.TypeStream)
	require.NoError(t, err)
}

func TestInitFromLocVersionConflictCompletes(t *testing.T) {
	const name = "sf1024_skip_source"
	t.Cleanup(func() { _, _ = streamProcessor.DropStream(name, ast.TypeStream) })
	_, err := streamProcessor.ExecReplaceStream(name, `CREATE STREAM sf1024_skip_source () WITH (DATASOURCE="demo", FORMAT="JSON", VERSION="2")`, ast.TypeStream)
	require.NoError(t, err)
	loc := t.TempDir()
	initFile := filepath.Join(loc, "init.json")
	require.NoError(t, os.WriteFile(initFile, []byte(`{"streams":{"sf1024_skip_source":"CREATE STREAM sf1024_skip_source () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\", VERSION=\"1\")"}}`), 0o644))
	require.NoError(t, initFromLoc(loc))
	info, err := os.Stat(initFile)
	require.NoError(t, err)
	require.Equal(t, info.ModTime().UnixMilli(), findInitializedTime(loc))
}

func TestInitFromLocInvalidJSONCompletes(t *testing.T) {
	loc := t.TempDir()
	initFile := filepath.Join(loc, "init.json")
	require.NoError(t, os.WriteFile(initFile, []byte("not JSON"), 0o644))
	require.NoError(t, initFromLoc(loc))
	info, err := os.Stat(initFile)
	require.NoError(t, err)
	require.Equal(t, info.ModTime().UnixMilli(), findInitializedTime(loc))
}

func TestInitFromLocInvalidSQLCompletes(t *testing.T) {
	loc := t.TempDir()
	initFile := filepath.Join(loc, "init.json")
	require.NoError(t, os.WriteFile(initFile, []byte(`{"streams":{"sf1024_bad_sql":"not SQL"}}`), 0o644))
	require.NoError(t, initFromLoc(loc))
	info, err := os.Stat(initFile)
	require.NoError(t, err)
	require.Equal(t, info.ModTime().UnixMilli(), findInitializedTime(loc))
}

func TestUnreadableInitDoesNotCreateMarker(t *testing.T) {
	loc := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(loc, "init.json"), 0o755))
	require.NoError(t, initFromLoc(loc))
	require.EqualValues(t, -1, findInitializedTime(loc))
}
