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

package fvt

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

// cleanupRequest registers deletion of a persistent resource created by
// this test. The strict and legacy profiles share the same workspace
// storage, so nothing created here may leak into the next process:
// failures surface instead of being silently dropped.
func cleanupRequest(t *testing.T, f func() (*http.Response, error)) {
	t.Helper()
	t.Cleanup(func() {
		resp, err := f()
		require.NoError(t, err)
		if resp != nil {
			defer resp.Body.Close()
			require.Less(t, resp.StatusCode, 300, "cleanup request failed")
		}
	})
}

// Strict-default file access coverage against the live in-process server.
// This test must run in its own process WITHOUT the
// KUIPER__BASIC__ALLOWEXTERNALFILEACCESS override (see the "Run fvt" CI
// step), so it observes the production default. It never mutates global
// config: flipping the switch at runtime would leak into the long-lived
// server goroutines and trip the race detector.
func TestFileAccessStrictDefault(t *testing.T) {
	require.False(t, conf.Config.Basic.AllowExternalFileAccess,
		"strict profile must run without external file access")

	dataDir, err := conf.GetDataLoc()
	require.NoError(t, err)
	inFile := filepath.Join(dataDir, "fvt_strict_in.lines")
	require.NoError(t, os.WriteFile(inFile, []byte("{}\n"), 0o644))
	// Register fixture removal first so it runs last, after all
	// rule/stream/confKey cleanups (t.Cleanup is LIFO).
	t.Cleanup(func() { _ = os.Remove(inFile) })
	outFile := filepath.Join(dataDir, "fvt_strict_out.log")
	t.Cleanup(func() { _ = os.Remove(outFile) })

	// Inside data dir: stream + rule creation succeed.
	resp, err := client.CreateStream(`{"sql": "CREATE STREAM fvt_strict_in () WITH (DATASOURCE=\"fvt_strict_in.lines\", FORMAT=\"json\", TYPE=\"file\")"}`)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	_, err = GetResponseText(resp)
	require.NoError(t, err)
	cleanupRequest(t, func() (*http.Response, error) {
		return client.DeleteStream("fvt_strict_in")
	})

	// 1. file sink outside the data dir must be denied with 400.
	outside := filepath.Join(t.TempDir(), "fvt_strict.json")
	resp, err = client.CreateRule(fmt.Sprintf(`{
		"id": "fvt_strict_deny_sink",
		"sql": "SELECT * FROM fvt_strict_in",
		"actions": [{"file": {"path": %q, "fileType": "lines", "format": "json", "rollingCount": 100}}]
	}`, outside))
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, err := GetResponseText(resp)
	require.NoError(t, err)
	assert.Contains(t, body, "file access denied")

	// 2. file source outside the data dir must be denied with 400.
	// confKey/stream setup itself is lazy and allowed; the rule triggers
	// source provisioning where the denial surfaces.
	resp, err = client.CreateConf("sources/file/confKeys/fvt_strict_etc", map[string]any{
		"fileType": "lines",
		"path":     outside,
	})
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	_, err = GetResponseText(resp)
	require.NoError(t, err)
	resp, err = client.CreateStream(`{"sql": "CREATE STREAM fvt_strict_etc () WITH (DATASOURCE=\"x.lines\", FORMAT=\"json\", TYPE=\"file\", CONF_KEY=\"fvt_strict_etc\")"}`)
	require.NoError(t, err)
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	_, err = GetResponseText(resp)
	require.NoError(t, err)
	cleanupRequest(t, func() (*http.Response, error) {
		return client.Delete("metadata/sources/file/confKeys/fvt_strict_etc")
	})
	cleanupRequest(t, func() (*http.Response, error) {
		return client.DeleteStream("fvt_strict_etc")
	})
	resp, err = client.CreateRule(`{
		"id": "fvt_strict_deny_src",
		"sql": "SELECT * FROM fvt_strict_etc",
		"actions": [{"log": {}}]
	}`)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
	body, err = GetResponseText(resp)
	require.NoError(t, err)
	assert.Contains(t, body, "file access denied")

	// 3. data-dir relative sink path must keep working end to end:
	// rule creation succeeds AND the output file actually lands.
	resp, err = client.CreateRule(`{
		"id": "fvt_strict_allow",
		"sql": "SELECT * FROM fvt_strict_in",
		"actions": [{"file": {"path": "fvt_strict_out.log", "fileType": "lines", "format": "json", "rollingCount": 100}}]
	}`)
	require.NoError(t, err)
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	_, err = GetResponseText(resp)
	require.NoError(t, err)
	cleanupRequest(t, func() (*http.Response, error) {
		return client.DeleteRule("fvt_strict_allow")
	})
	require.Eventually(t, func() bool {
		_, err := os.Stat(outFile)
		return err == nil
	}, 5*time.Second, 100*time.Millisecond, "output file was not written")
}
