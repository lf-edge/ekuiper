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
)

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
	_, err = ValidateFilePath("/etc/passwd")
	assert.ErrorContains(t, err, "file access denied")

	// A symlink planted inside the data dir pointing outside must not
	// validate, even though it is addressed through the data dir.
	outside := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("s"), 0o644))
	require.NoError(t, os.Symlink(outside, filepath.Join(realBase, "data", "evil")))
	_, err = ValidateFilePath("evil/secret.txt")
	assert.ErrorContains(t, err, "file access denied")
}
