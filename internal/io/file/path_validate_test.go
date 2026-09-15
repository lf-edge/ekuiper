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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// Existing functional tests in this package use tmp//tmp directories by
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

func TestSandboxSinkProvision(t *testing.T) {
	withRestrictedAccess(t)
	ctx := mockContext.NewMockContext("sandbox", "sink")

	// Absolute path outside the data dir must be rejected.
	s := &fileSink{}
	err := s.Provision(ctx, map[string]any{
		"path":               "/tmp/sandbox_sink.log",
		"rollingCount":       1,
		"rollingNamePattern": "none",
	})
	assert.ErrorContains(t, err, "file access denied")

	// Traversal in a relative path must be rejected.
	s = &fileSink{}
	err = s.Provision(ctx, map[string]any{
		"path":               "../../etc/passwd",
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
		"datasource": "/etc/passwd",
		"path":       dataDir,
		"fileType":   "json",
	})
	assert.ErrorContains(t, err, "datasource")

	// Absolute path outside the data dir must be rejected.
	fs = &Source{}
	err = fs.Provision(ctx, map[string]any{
		"datasource": "sandbox_src.json",
		"path":       "/tmp",
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
		"moveTo":          "/tmp/sandbox_moved",
	})
	assert.ErrorContains(t, err, "file access denied")
}

func TestSandboxSymlinkEscape(t *testing.T) {
	withRestrictedAccess(t)
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)

	// Plant a symlink inside the data dir pointing outside.
	link := filepath.Join(dataDir, "sandbox_link")
	os.Remove(link)
	require.NoError(t, os.Symlink("/etc", link))
	t.Cleanup(func() { os.Remove(link) })

	_, err = validateFilePath(filepath.Join("sandbox_link", "passwd"))
	assert.ErrorContains(t, err, "file access denied")
}
