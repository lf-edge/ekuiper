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

package httpx

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

// A truncated body must fail the download and remove the partial file
// inside the sandbox (never via a global remove).
func TestDownloadFileCleansUpOnCopyError(t *testing.T) {
	origConfig := conf.Config
	t.Cleanup(func() { conf.Config = origConfig })
	conf.Config = &model.KuiperConf{}
	// Allow the guarded client to dial the local test server.
	conf.Config.Basic.EnablePrivateNet = true

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000000")
		_, _ = w.Write([]byte("partial"))
		// Return early: the client sees unexpected EOF.
	}))
	defer srv.Close()

	targetDir := t.TempDir()
	_, err := DownloadFile(targetDir, "partial.txt", srv.URL)
	require.Error(t, err)
	_, statErr := os.Stat(filepath.Join(targetDir, "partial.txt"))
	assert.True(t, os.IsNotExist(statErr), "partial file must be cleaned up")
}
