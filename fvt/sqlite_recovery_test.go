// Copyright 2024 EMQ Technologies Co., Ltd.
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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func getFreePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func (s *ServerTestSuite) TestResetNormalModeNoForce() {
	resp, err := client.Post("reset", "")
	s.Require().NoError(err)
	defer resp.Body.Close()
	s.Require().Equal(http.StatusBadRequest, resp.StatusCode)
}

func TestSqliteRecoveryE2E(t *testing.T) {
	tempWorkspace := t.TempDir()

	// 1. Build kuiperd binary for E2E testing
	binPath := filepath.Join(tempWorkspace, "kuiperd")
	buildCmd := exec.Command("go", "build", "-o", binPath, "./cmd/kuiperd")
	buildCmd.Dir = PWD
	output, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "failed to build kuiperd: %s", string(output))

	// 2. Prepare test directory structure
	etcDir := filepath.Join(tempWorkspace, "etc")
	dataDir := filepath.Join(tempWorkspace, "data")
	logDir := filepath.Join(tempWorkspace, "log")
	pluginsDir := filepath.Join(tempWorkspace, "plugins")
	require.NoError(t, os.MkdirAll(etcDir, 0o755))
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.MkdirAll(logDir, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(pluginsDir, "sinks"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(pluginsDir, "sources"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(pluginsDir, "functions"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(pluginsDir, "portable"), 0o755))

	testRestPort := getFreePort(t)
	testRpcPort := getFreePort(t)
	testPrometheusPort := getFreePort(t)
	kuiperYaml := fmt.Sprintf(`basic:
  logLevel: info
  consoleLog: true
  fileLog: false
  ip: 127.0.0.1
  port: %d
  restIp: 127.0.0.1
  restPort: %d
  prometheus: false
  prometheusPort: %d
  pprof: false
  allowExternalFileAccess: true
store:
  type: sqlite
`, testRpcPort, testRestPort, testPrometheusPort)
	require.NoError(t, os.WriteFile(filepath.Join(etcDir, "kuiper.yaml"), []byte(kuiperYaml), 0o644))

	// 3. Put corrupted SQLite file, initialized marker, and factory init.json in data dir
	dbPath := filepath.Join(dataDir, "sqliteKV.db")
	require.NoError(t, os.WriteFile(dbPath, []byte("this is corrupted sqlite data page"), 0o600))
	initMarker := filepath.Join(dataDir, "initialized123456")
	require.NoError(t, os.WriteFile(initMarker, []byte(""), 0o600))
	initJsonPath := filepath.Join(dataDir, "init.json")
	initJsonContent := `{"streams":{"init_stream":"create stream init_stream () WITH (DATASOURCE=\"init_demo\", FORMAT=\"json\");"}}`
	require.NoError(t, os.WriteFile(initJsonPath, []byte(initJsonContent), 0o600))

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", testRestPort)

	// 4. Start kuiperd with corrupted SQLite database
	cmd := exec.Command(binPath, "-etc", etcDir, "-data", dataDir, "-log", logDir, "-plugins", pluginsDir)
	cmd.Dir = tempWorkspace
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())

	// Wait for REST API to become ready (in degraded mode)
	require.True(t, waitForReady(baseURL+"/ping", 15*time.Second), "degraded server did not become ready")

	// 5. Verify degraded mode behavior
	// 5.1 /ping returns 200 OK
	resp, err := http.Get(baseURL + "/ping")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// 5.2 / returns degraded status and reason
	resp, err = http.Get(baseURL + "/")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	var rootInfo map[string]any
	require.NoError(t, json.Unmarshal(body, &rootInfo))
	require.Equal(t, true, rootInfo["degraded"])
	require.Equal(t, "degraded", rootInfo["status"])
	require.NotEmpty(t, rootInfo["degradedReason"])
	require.NotEmpty(t, rootInfo["reason"])
	require.Equal(t, rootInfo["degradedReason"], rootInfo["reason"])

	// 5.3 Corrupted database file MUST NOT be deleted automatically on startup
	dbFi, statErr := os.Stat(dbPath)
	require.NoError(t, statErr, "corrupted database must be kept intact in degraded mode")
	require.Equal(t, int64(len("this is corrupted sqlite data page")), dbFi.Size())

	// 5.4 /rules returns 503 Service Unavailable
	resp, err = http.Get(baseURL + "/rules")
	require.NoError(t, err)
	require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	resp.Body.Close()

	// 6. Call POST /reset to recover (returns 202 Accepted)
	resp, err = http.Post(baseURL+"/reset", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	resp.Body.Close()

	// 7. Verify process exits automatically
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	select {
	case <-done:
		// Process exited successfully
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("kuiperd process did not exit after reset within 10s")
	}

	// 8. Verify corrupted database and initialized files are deleted, BUT init.json is preserved
	_, err = os.Stat(dbPath)
	require.True(t, os.IsNotExist(err), "corrupted DB file should have been deleted")
	_, err = os.Stat(initMarker)
	require.True(t, os.IsNotExist(err), "initialized marker should have been deleted")
	readInitJson, err := os.ReadFile(initJsonPath)
	require.NoError(t, err, "init.json must be preserved after reset")
	require.Equal(t, initJsonContent, string(readInitJson))

	// 9. Start kuiperd again (simulating supervisor / systemd restart)
	cmd2 := exec.Command(binPath, "-etc", etcDir, "-data", dataDir, "-log", logDir, "-plugins", pluginsDir)
	cmd2.Dir = tempWorkspace
	cmd2.Stdout = os.Stdout
	cmd2.Stderr = os.Stderr
	require.NoError(t, cmd2.Start())
	defer func() {
		_ = cmd2.Process.Kill()
	}()

	// Wait for server to become ready
	require.True(t, waitForReady(baseURL+"/ping", 15*time.Second), "restarted server did not become ready")

	// 10. Verify normal mode behavior after recovery
	resp, err = http.Get(baseURL + "/")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	rootInfo = make(map[string]any)
	require.NoError(t, json.Unmarshal(body, &rootInfo))
	require.Equal(t, false, rootInfo["degraded"], "server should no longer be in degraded mode")
	require.Equal(t, "running", rootInfo["status"])

	// init_stream was recreated from the preserved init.json
	resp, err = http.Get(baseURL + "/streams/init_stream")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()

	// Normal mode reset without force is rejected
	resp, err = http.Post(baseURL+"/reset", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusBadRequest, resp.StatusCode)
	resp.Body.Close()

	// 11. Normal mode reset with force=true succeeds (returns 202 Accepted) and exits cleanly
	resp, err = http.Post(baseURL+"/reset?force=true", "application/json", nil)
	require.NoError(t, err)
	require.Equal(t, http.StatusAccepted, resp.StatusCode)
	resp.Body.Close()

	done2 := make(chan error, 1)
	go func() {
		done2 <- cmd2.Wait()
	}()
	select {
	case <-done2:
		// Process exited successfully
	case <-time.After(10 * time.Second):
		_ = cmd2.Process.Kill()
		t.Fatal("kuiperd process did not exit after normal mode reset within 10s")
	}
}

func TestAuxStoreBestEffortE2E(t *testing.T) {
	tempWorkspace := t.TempDir()

	binPath := filepath.Join(tempWorkspace, "kuiperd")
	buildCmd := exec.Command("go", "build", "-o", binPath, "./cmd/kuiperd")
	buildCmd.Dir = PWD
	output, err := buildCmd.CombinedOutput()
	require.NoError(t, err, "failed to build kuiperd: %s", string(output))

	etcDir := filepath.Join(tempWorkspace, "etc")
	dataDir := filepath.Join(tempWorkspace, "data")
	logDir := filepath.Join(tempWorkspace, "log")
	pluginsDir := filepath.Join(tempWorkspace, "plugins")
	require.NoError(t, os.MkdirAll(etcDir, 0o755))
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.MkdirAll(logDir, 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(pluginsDir, "sinks"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(pluginsDir, "sources"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(pluginsDir, "functions"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(pluginsDir, "portable"), 0o755))

	testRestPort := getFreePort(t)
	testRpcPort := getFreePort(t)
	testPrometheusPort := getFreePort(t)
	kuiperYaml := fmt.Sprintf(`basic:
  logLevel: info
  consoleLog: true
  fileLog: false
  ip: 127.0.0.1
  port: %d
  restIp: 127.0.0.1
  restPort: %d
  prometheus: false
  prometheusPort: %d
  pprof: false
  allowExternalFileAccess: true
store:
  type: sqlite
`, testRpcPort, testRestPort, testPrometheusPort)
	require.NoError(t, os.WriteFile(filepath.Join(etcDir, "kuiper.yaml"), []byte(kuiperYaml), 0o644))

	// Put corrupted auxiliary database checkpoint.db in dataDir (main sqliteKV.db is NOT corrupted)
	auxPath := filepath.Join(dataDir, "checkpoint.db")
	require.NoError(t, os.WriteFile(auxPath, []byte("corrupted checkpoint auxiliary db"), 0o600))

	baseURL := fmt.Sprintf("http://127.0.0.1:%d", testRestPort)

	cmd := exec.Command(binPath, "-etc", etcDir, "-data", dataDir, "-log", logDir, "-plugins", pluginsDir)
	cmd.Dir = tempWorkspace
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	defer func() {
		_ = cmd.Process.Kill()
	}()

	// Wait for server to become ready
	require.True(t, waitForReady(baseURL+"/ping", 15*time.Second), "server did not become ready")

	// Verify server is in RUNNING mode (not degraded), but reports storage error for checkpoint.db
	resp, err := http.Get(baseURL + "/")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)

	var rootInfo map[string]any
	require.NoError(t, json.Unmarshal(body, &rootInfo))
	require.Equal(t, false, rootInfo["degraded"], "aux store corruption must not put server in degraded mode")
	require.Equal(t, "running", rootInfo["status"])

	// Corrupted auxiliary database file MUST NOT be deleted
	fi, statErr := os.Stat(auxPath)
	require.NoError(t, statErr, "corrupted auxiliary database must be kept intact")
	require.Equal(t, int64(len("corrupted checkpoint auxiliary db")), fi.Size())

	// Rules endpoint is accessible (normal mode)
	resp, err = http.Get(baseURL + "/rules")
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)
	resp.Body.Close()
}

func waitForReady(url string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
