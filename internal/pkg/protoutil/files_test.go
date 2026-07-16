// Copyright 2026 EMQ Technologies Co., Ltd.
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

package protoutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCollectFiles(t *testing.T) {
	root := t.TempDir()
	protoFile := filepath.Join(root, "schema.proto")
	require.NoError(t, os.WriteFile(protoFile, []byte(`message Test {}`), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(root, "ignored.txt"), nil, 0o600))

	t.Run("single file", func(t *testing.T) {
		files, err := CollectFiles(protoFile)
		require.NoError(t, err)
		assert.Equal(t, []string{protoFile}, files)
	})

	t.Run("directory", func(t *testing.T) {
		files, err := CollectFiles(root)
		require.NoError(t, err)
		assert.Equal(t, []string{protoFile}, files)
	})

	t.Run("empty directory", func(t *testing.T) {
		_, err := CollectFiles(t.TempDir())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no .proto files found")
	})

	t.Run("not found", func(t *testing.T) {
		_, err := CollectFiles(filepath.Join(root, "missing.proto"))
		require.Error(t, err)
	})
}

func TestResolveFiles(t *testing.T) {
	root := t.TempDir()
	absoluteFile := filepath.Join(root, "nested", "schema.proto")
	require.NoError(t, os.MkdirAll(filepath.Dir(absoluteFile), 0o755))
	require.NoError(t, os.WriteFile(absoluteFile, []byte(`message Test {}`), 0o600))

	t.Run("absolute path", func(t *testing.T) {
		files, err := ResolveFiles([]string{root}, []string{absoluteFile})
		require.NoError(t, err)
		assert.Equal(t, []string{filepath.ToSlash(filepath.Join("nested", "schema.proto"))}, files)
	})

	t.Run("relative path", func(t *testing.T) {
		files, err := ResolveFiles([]string{root}, []string{"nested/schema.proto"})
		require.NoError(t, err)
		assert.Equal(t, []string{"nested/schema.proto"}, files)
	})

	t.Run("empty import paths", func(t *testing.T) {
		files, err := ResolveFiles([]string{""}, []string{absoluteFile})
		require.NoError(t, err)
		assert.Equal(t, []string{absoluteFile}, files)
	})

	t.Run("outside import paths", func(t *testing.T) {
		_, err := ResolveFiles([]string{t.TempDir()}, []string{absoluteFile})
		require.Error(t, err)
	})
}
