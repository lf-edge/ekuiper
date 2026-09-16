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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	mockContext "github.com/lf-edge/ekuiper/v2/pkg/mock/context"
)

func noopIngest(api.StreamContext, any, map[string]any, time.Time) {}

func TestParseFileDeniesOutside(t *testing.T) {
	withRestrictedAccess(t)
	ctx := mockContext.NewMockContext("sandbox", "source")
	fs := &Source{config: &SourceConfig{FileType: "json"}}
	var gotErr error
	fs.parseFile(ctx, "/etc/passwd",
		noopIngest,
		func(_ api.StreamContext, err error) { gotErr = err })
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "file access denied")
}

func TestParseFileMoveTargetDenied(t *testing.T) {
	withRestrictedAccess(t)
	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	src := filepath.Join(dataDir, "sandbox_rename_src.json")
	require.NoError(t, os.WriteFile(src, []byte(`{}`), 0o644))
	t.Cleanup(func() { os.Remove(src) })

	ctx := mockContext.NewMockContext("sandbox", "source")
	fs := &Source{config: &SourceConfig{
		FileType:        "json",
		ActionAfterRead: 2,
		MoveTo:          filepath.Join(os.TempDir(), "sandbox_nope_ekuiper"),
	}}
	var gotErr error
	fs.parseFile(ctx, src,
		noopIngest,
		func(_ api.StreamContext, err error) { gotErr = err })
	require.Error(t, gotErr)
	assert.Contains(t, gotErr.Error(), "file access denied")
	// The source file must survive: no move happened.
	_, statErr := os.Stat(src)
	assert.NoError(t, statErr)
}

func TestCreateFileWriterDeniesOutside(t *testing.T) {
	withRestrictedAccess(t)
	ctx := mockContext.NewMockContext("sandbox", "sink")
	m := &fileSink{}
	_, err := m.createFileWriter(ctx, "/etc/sandbox_evil.log", LINES_TYPE, "", "", "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "file access denied")
}

type evilDynamicTuple struct{ raw []byte }

func (e *evilDynamicTuple) Raw() []byte                        { return e.raw }
func (e *evilDynamicTuple) Replace(b []byte)                   { e.raw = b }
func (e *evilDynamicTuple) DynamicProps(string) (string, bool) { return "/etc/evil_sink.log", true }
func (e *evilDynamicTuple) AllProps() map[string]string        { return nil }

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
	err := m.Collect(ctx, &evilDynamicTuple{raw: []byte(`{"a":1}`)})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "file access denied")
}
