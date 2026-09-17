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
)

func TestWriteInitializedKeepsOldMarkerUntilNewIsReady(t *testing.T) {
	loc := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(loc, "initialized123"), nil, 0o644))
	require.Error(t, writeInitialized(filepath.Join(loc, "missing"), 456))
	require.EqualValues(t, 123, findInitializedTime(loc))
	require.NoError(t, writeInitialized(loc, 456))
	require.EqualValues(t, 456, findInitializedTime(loc))
	require.NoError(t, os.WriteFile(filepath.Join(loc, "initialized123"), nil, 0o644))
	require.EqualValues(t, -1, findInitializedTime(loc))
	require.NoError(t, writeInitialized(loc, 456))
	require.EqualValues(t, 456, findInitializedTime(loc))
}

func TestInvalidInitJSONIsMarkedCompleted(t *testing.T) {
	loc := t.TempDir()
	initFile := filepath.Join(loc, "init.json")
	require.NoError(t, os.WriteFile(initFile, []byte("not JSON"), 0o644))
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
