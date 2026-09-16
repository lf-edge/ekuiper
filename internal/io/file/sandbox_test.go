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

package file

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// Existing functional tests in this package use tmp directories by
// design, so external access stays on for them. Sandbox restriction is
// covered by the TestSandbox* tests below which explicitly turn the switch
// off.
func TestMain(m *testing.M) {
	if conf.Config == nil {
		conf.InitConf()
	}
	conf.Config.Basic.AllowExternalFileAccess = true
	os.Exit(m.Run())
}

func withRestrictedAccess(t *testing.T) {
	t.Helper()
	if conf.Config == nil {
		conf.Config = &model.KuiperConf{}
	}
	old := conf.Config.Basic.AllowExternalFileAccess
	conf.Config.Basic.AllowExternalFileAccess = false
	t.Cleanup(func() {
		conf.Config.Basic.AllowExternalFileAccess = old
	})
}

// outside returns a path guaranteed outside the data dir under test.
func outside(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "secret.txt")
}

func noopIngest(api.StreamContext, any, map[string]any, time.Time) {}

type evilDynamicTuple struct {
	raw  []byte
	path string
}

func (e *evilDynamicTuple) Raw() []byte                        { return e.raw }
func (e *evilDynamicTuple) Replace(b []byte)                   { e.raw = b }
func (e *evilDynamicTuple) DynamicProps(string) (string, bool) { return e.path, true }
func (e *evilDynamicTuple) AllProps() map[string]string        { return nil }

func TestSandboxSinkProvision(t *testing.T) {
	withRestrictedAccess(t)
	ctx := mockContext.NewMockContext("sandbox", "sink")

	// Absolute path outside the data dir must be rejected.
	s := &fileSink{}
	err := s.Provision(ctx, map[string]any{
		"path":               outside(t),
		"rollingCount":       1,
		"rollingNamePattern": "none",
	})
	assert.ErrorContains(t, err, "file access denied")

	// Traversal in a relative path must be rejected.
	s = &fileSink{}
	err = s.Provision(ctx, map[string]any{
		"path":               filepath.Join("..", "secret.txt"),
		"rollingCount":       1,
		"rollingNamePattern": "none",
	})
	assert.ErrorContains(t, err, "file access denied")

	// A plain relative path resolves inside the data dir and is allowed.
	s = &fileSink{}
	err = s.Provision(ctx, map[string]any{
		"path":               "sandbox_sink.log",
		"rollingCount":       1,
		"rollingNamePattern": "none",
	})
	assert.NoError(t, err)

	// An absolute path inside the data dir is allowed.
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	s = &fileSink{}
	err = s.Provision(ctx, map[string]any{
		"path":               filepath.Join(dataDir, "sandbox_sink_abs.log"),
		"rollingCount":       1,
		"rollingNamePattern": "none",
	})
	assert.NoError(t, err)
}

func TestSandboxSourceProvision(t *testing.T) {
	withRestrictedAccess(t)
	ctx := mockContext.NewMockContext("sandbox", "source")
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)

	// Prepare a legit file inside the data dir.
	legit := filepath.Join(dataDir, "sandbox_src.json")
	require.NoError(t, os.WriteFile(legit, []byte("{}"), 0o644))
	t.Cleanup(func() { os.Remove(legit) })

	// datasource with traversal must be rejected.
	fs := &Source{}
	err = fs.Provision(ctx, map[string]any{
		"datasource": "../secret.json",
		"path":       dataDir,
		"fileType":   "json",
	})
	assert.ErrorContains(t, err, "datasource")

	// Absolute datasource must be rejected.
	fs = &Source{}
	err = fs.Provision(ctx, map[string]any{
		"datasource": outside(t),
		"path":       dataDir,
		"fileType":   "json",
	})
	assert.ErrorContains(t, err, "datasource")

	// Absolute path outside the data dir must be rejected.
	fs = &Source{}
	err = fs.Provision(ctx, map[string]any{
		"datasource": "sandbox_src.json",
		"path":       t.TempDir(),
		"fileType":   "json",
	})
	assert.ErrorContains(t, err, "file access denied")

	// Legit file inside the data dir passes.
	fs = &Source{}
	err = fs.Provision(ctx, map[string]any{
		"datasource": "sandbox_src.json",
		"path":       dataDir,
		"fileType":   "json",
	})
	assert.NoError(t, err)

	// moveTo outside the data dir must be rejected.
	fs = &Source{}
	err = fs.Provision(ctx, map[string]any{
		"datasource":      "sandbox_src.json",
		"path":            dataDir,
		"fileType":        "json",
		"actionAfterRead": 2,
		"moveTo":          t.TempDir(),
	})
	assert.ErrorContains(t, err, "file access denied")
}

// A symlink planted inside a provisioned directory after Provision must
// be denied at read time through the real Load path.
func TestLoadDeniesPlantedSymlink(t *testing.T) {
	withRestrictedAccess(t)
	ctx := mockContext.NewMockContext("sandbox", "source")
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	watch := filepath.Join(dataDir, "sandbox_watch")
	require.NoError(t, os.MkdirAll(watch, 0o755))
	t.Cleanup(func() { os.RemoveAll(watch) })

	fs := &Source{}
	require.NoError(t, fs.Provision(ctx, map[string]any{
		"datasource": "",
		"path":       watch,
		"fileType":   "json",
	}))

	// Attacker plants a file symlink after provisioning.
	planted := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(planted, "secret.txt"), []byte("{}"), 0o644))
	require.NoError(t, os.Symlink(filepath.Join(planted, "secret.txt"), filepath.Join(watch, "evil.json")))

	var errs []error
	fs.Load(ctx, noopIngest, func(_ api.StreamContext, err error) {
		errs = append(errs, err)
	})
	require.NotEmpty(t, errs)
	denied := false
	for _, e := range errs {
		if e != nil && strings.Contains(e.Error(), "file access denied") {
			denied = true
		}
	}
	assert.True(t, denied, "planted symlink read must be denied, got %v", errs)
}

// Swapping a provisioned moveTo directory for an escaping symlink after
// Provision must deny the move through the real Load path.
func TestLoadDeniesSwappedMoveTarget(t *testing.T) {
	withRestrictedAccess(t)
	ctx := mockContext.NewMockContext("sandbox", "source")
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	watch := filepath.Join(dataDir, "sandbox_watch_move")
	require.NoError(t, os.MkdirAll(watch, 0o755))
	t.Cleanup(func() { os.RemoveAll(watch) })

	realFile := filepath.Join(watch, "real.json")
	require.NoError(t, os.WriteFile(realFile, []byte(`{}`), 0o644))
	moved := filepath.Join(watch, "moved")

	fs := &Source{}
	require.NoError(t, fs.Provision(ctx, map[string]any{
		"datasource":      "real.json",
		"path":            watch,
		"fileType":        "json",
		"actionAfterRead": 2,
		"moveTo":          moved,
	}))

	// Attacker replaces the provisioned moveTo dir with an escaping symlink.
	require.NoError(t, os.RemoveAll(moved))
	require.NoError(t, os.Symlink(t.TempDir(), moved))

	var errs []error
	fs.Load(ctx, noopIngest, func(_ api.StreamContext, err error) {
		errs = append(errs, err)
	})
	require.NotEmpty(t, errs)
	denied := false
	for _, e := range errs {
		if e != nil && strings.Contains(e.Error(), "file access denied") {
			denied = true
		}
	}
	assert.True(t, denied, "swapped moveTo must be denied, got %v", errs)
	// The source file must survive: no move happened.
	_, statErr := os.Stat(realFile)
	assert.NoError(t, statErr)
}

func TestSinkCollectDeniesDynamicTraversal(t *testing.T) {
	withRestrictedAccess(t)
	ctx := mockContext.NewMockContext("sandbox", "sink")
	m := &fileSink{}
	require.NoError(t, m.Provision(ctx, map[string]any{
		"path":               "sandbox_collect.log",
		"fileType":           LINES_TYPE,
		"format":             "json",
		"rollingCount":       1,
		"rollingNamePattern": "none",
	}))
	err := m.Collect(ctx, &evilDynamicTuple{raw: []byte(`{"a":1}`), path: outside(t)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "file access denied")
}
