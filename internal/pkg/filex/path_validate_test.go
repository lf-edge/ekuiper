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

package filex

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func withFlag(t *testing.T, allow bool) {
	t.Helper()
	if conf.Config == nil {
		conf.Config = &model.KuiperConf{}
	}
	old := conf.Config.Basic.AllowExternalFileAccess
	conf.Config.Basic.AllowExternalFileAccess = allow
	t.Cleanup(func() {
		conf.Config.Basic.AllowExternalFileAccess = old
	})
}

// outside returns a path guaranteed outside any data dir under test.
func outside(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "secret.txt")
}

func TestValidateFilePathModes(t *testing.T) {
	withFlag(t, false)

	_, err := ValidateFilePath("")
	assert.ErrorContains(t, err, "path must be set")

	withFlag(t, true)
	// Flag on: no sandbox, absolute and relative pass through.
	got, err := ValidateFilePath(outside(t))
	require.NoError(t, err)
	assert.True(t, filepath.IsAbs(got))

	got, err = ValidateFilePath("rel/foo.log")
	require.NoError(t, err)
	abs, _ := filepath.Abs("rel/foo.log")
	assert.Equal(t, abs, got)
}

// The data directory itself may sit behind a symlink (e.g.
// /opt/ekuiper-current -> /opt/ekuiper-2.2.0). Files addressed through the
// symlinked base must still validate: the base has to be canonicalized
// before comparing against the canonicalized target.
func TestValidateFilePathSymlinkedBase(t *testing.T) {
	// Fail-closed when the switch is off; nil config counts as off.
	oldConf := conf.Config
	conf.Config = nil
	t.Cleanup(func() { conf.Config = oldConf })

	oldTesting := conf.IsTesting
	conf.IsTesting = false
	t.Cleanup(func() { conf.IsTesting = oldTesting })

	realBase := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(realBase, "data"), 0o755))
	legit := filepath.Join(realBase, "data", "foo.log")
	require.NoError(t, os.WriteFile(legit, []byte("x"), 0o644))

	linkBase := filepath.Join(t.TempDir(), "ekuiper-current")
	require.NoError(t, os.Symlink(realBase, linkBase))

	oldEnv, hadEnv := os.LookupEnv(conf.KuiperBaseKey)
	require.NoError(t, os.Setenv(conf.KuiperBaseKey, linkBase))
	t.Cleanup(func() {
		if hadEnv {
			os.Setenv(conf.KuiperBaseKey, oldEnv)
		} else {
			os.Unsetenv(conf.KuiperBaseKey)
		}
	})

	// Relative path through the symlinked base must be accepted.
	got, err := ValidateFilePath("foo.log")
	require.NoError(t, err)
	assert.Equal(t, legit, got)

	// Absolute path through the symlinked base must be accepted too.
	got, err = ValidateFilePath(filepath.Join(linkBase, "data", "foo.log"))
	require.NoError(t, err)
	assert.Equal(t, legit, got)

	// Genuine escapes are still rejected.
	_, err = ValidateFilePath(filepath.Join(linkBase, "data", "..", "secret.txt"))
	assert.ErrorContains(t, err, "file access denied")
	_, err = ValidateFilePath(outside(t))
	assert.ErrorContains(t, err, "file access denied")

	// A symlink planted inside the data dir pointing outside must not
	// validate, even though it is addressed through the data dir.
	planted := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(planted, "secret.txt"), []byte("s"), 0o644))
	require.NoError(t, os.Symlink(planted, filepath.Join(realBase, "data", "evil")))
	_, err = ValidateFilePath("evil/secret.txt")
	assert.ErrorContains(t, err, "file access denied")
}

// The base data directory itself may not exist yet (absolute path mode
// performs no existence check) while an ancestor is a symlink. Both sides
// must still resolve through the same resolver instead of degrading the
// comparison to non-canonical base vs canonical target.
func TestValidateFilePathNonexistentSymlinkedBase(t *testing.T) {
	oldConf := conf.Config
	conf.Config = nil
	t.Cleanup(func() { conf.Config = oldConf })

	oldTesting := conf.IsTesting
	conf.IsTesting = false
	t.Cleanup(func() { conf.IsTesting = oldTesting })

	oldLoadType := conf.PathConfig.LoadFileType
	oldDirs := conf.PathConfig.Dirs
	t.Cleanup(func() {
		conf.PathConfig.LoadFileType = oldLoadType
		conf.PathConfig.Dirs = oldDirs
	})

	realBase := t.TempDir()
	linkBase := filepath.Join(t.TempDir(), "ekuiper-link")
	require.NoError(t, os.Symlink(realBase, linkBase))

	// Map the data dir through the symlink without creating it.
	conf.PathConfig.LoadFileType = "absolute"
	conf.PathConfig.Dirs = map[string]string{"data": filepath.Join(linkBase, "data")}

	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	// The point of this test: the base must NOT exist, otherwise it
	// silently degrades into the ordinary symlinked-base case above.
	_, statErr := os.Lstat(dataDir)
	require.ErrorIs(t, statErr, os.ErrNotExist)

	got, err := ValidateFilePath("foo.log")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(realBase, "data", "foo.log"), got)

	// Escapes through the missing base are still rejected.
	_, err = ValidateFilePath("../secret.txt")
	assert.ErrorContains(t, err, "file access denied")
}

func TestValidateFileName(t *testing.T) {
	withFlag(t, false)

	assert.NoError(t, ValidateFileName(""))
	assert.NoError(t, ValidateFileName("lookup.json"))
	assert.NoError(t, ValidateFileName("a/b.json"))
	assert.ErrorContains(t, ValidateFileName(outside(t)), "absolute path")
	assert.ErrorContains(t, ValidateFileName(".."), "traversal")
	assert.ErrorContains(t, ValidateFileName("../secret.json"), "traversal")

	withFlag(t, true)
	assert.NoError(t, ValidateFileName("../secret.json"))
	assert.NoError(t, ValidateFileName(outside(t)))
}

func TestOpenUnderRoot(t *testing.T) {
	withFlag(t, false)
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "ok.txt"), []byte("hello"), 0o644))

	rc, err := OpenUnderRoot(root, "ok.txt")
	require.NoError(t, err)
	buf := make([]byte, 5)
	_, err = rc.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(buf))
	require.NoError(t, rc.Close())

	_, err = OpenUnderRoot(root, filepath.Join("..", "evil.txt"))
	assert.ErrorContains(t, err, "file access denied")

	_, err = OpenUnderRoot(root, outside(t))
	assert.ErrorContains(t, err, "file access denied")

	_, err = OpenUnderRoot(root, "missing.txt")
	assert.Error(t, err)

	// The root containment holds regardless of the global switch.
	withFlag(t, true)
	_, err = OpenUnderRoot(root, filepath.Join("..", "evil.txt"))
	assert.ErrorContains(t, err, "file access denied")
	_, err = OpenUnderRoot(root, outside(t))
	assert.ErrorContains(t, err, "file access denied")
}
