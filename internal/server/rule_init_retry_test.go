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
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInitFromLocKeepsMarkerUntilSuccessful(t *testing.T) {
	loc := t.TempDir()
	initFile := filepath.Join(loc, "init.json")
	require.NoError(t, os.WriteFile(initFile, []byte(`{"streams":{}}`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(loc, "initialized123"), nil, 0o644))
	calls := 0
	importer := func([]byte) ([]int, bool, error) {
		calls++
		return []int{0, 0, 0}, calls == 1, nil
	}
	require.NoError(t, initFromLocWith(loc, importer))
	require.EqualValues(t, 123, findInitializedTime(loc))
	require.NoError(t, initFromLocWith(loc, importer))
	info, err := os.Stat(initFile)
	require.NoError(t, err)
	require.Equal(t, info.ModTime().UnixMilli(), findInitializedTime(loc))
	require.Equal(t, 2, calls)
}

func TestInitFromLocVersionSkipCompletes(t *testing.T) {
	loc := t.TempDir()
	initFile := filepath.Join(loc, "init.json")
	require.NoError(t, os.WriteFile(initFile, []byte(`{"rules":{}}`), 0o644))
	require.NoError(t, initFromLocWith(loc, func([]byte) ([]int, bool, error) {
		return []int{0, 0, 0}, false, nil
	}))
	info, err := os.Stat(initFile)
	require.NoError(t, err)
	require.Equal(t, info.ModTime().UnixMilli(), findInitializedTime(loc))
}

func TestInitFromLocParseErrorDoesNotCreateMarker(t *testing.T) {
	loc := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(loc, "init.json"), []byte("not JSON"), 0o644))
	require.NoError(t, initFromLocWith(loc, func([]byte) ([]int, bool, error) {
		return nil, false, errors.New("invalid import file")
	}))
	require.EqualValues(t, -1, findInitializedTime(loc))
}

func TestUnreadableInitDoesNotCreateMarker(t *testing.T) {
	loc := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(loc, "init.json"), 0o755))
	require.NoError(t, initFromLocWith(loc, nil))
	require.EqualValues(t, -1, findInitializedTime(loc))
}
