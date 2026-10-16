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

package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestResetHandler_Validation(t *testing.T) {
	// Ensure cleanup of any reset state
	isResetRequested.Store(false)
	defer isResetRequested.Store(false)

	// Isolate stop notification to prevent background goroutine from affecting server
	origStop := onResetStop
	stopCh := make(chan struct{}, 2)
	onResetStop = func() {
		stopCh <- struct{}{}
	}
	defer func() {
		onResetStop = origStop
	}()

	waitStop := func() {
		select {
		case <-stopCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for reset stop callback")
		}
	}

	origConf := conf.Config
	mockConf := &model.KuiperConf{}
	mockConf.Store.Type = "sqlite"
	mockConf.Store.ExtStateType = "sqlite"
	conf.Config = mockConf
	defer func() {
		conf.Config = origConf
	}()

	isDegraded = false
	degradedReason = ""

	// 1. Normal mode without force=true should return 400
	req := httptest.NewRequest(http.MethodPost, "/reset", nil)
	w := httptest.NewRecorder()
	resetHandler(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)

	// 2. Unsupported main store backend should return 400
	conf.Config.Store.Type = "redis"
	req = httptest.NewRequest(http.MethodPost, "/reset?force=true", nil)
	w = httptest.NewRecorder()
	resetHandler(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	conf.Config.Store.Type = "sqlite"

	// 2.1 Unsupported extState backend should return 400
	conf.Config.Store.ExtStateType = "redis"
	req = httptest.NewRequest(http.MethodPost, "/reset?force=true", nil)
	w = httptest.NewRecorder()
	resetHandler(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	conf.Config.Store.ExtStateType = "sqlite"

	// 3. Normal mode with force=true should succeed (returns 202)
	req = httptest.NewRequest(http.MethodPost, "/reset?force=true", nil)
	w = httptest.NewRecorder()
	resetHandler(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)
	var resp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	require.Contains(t, resp["message"], "reset process initiated")
	waitStop()

	// 4. Duplicate reset should return 409 Conflict
	req = httptest.NewRequest(http.MethodPost, "/reset?force=true", nil)
	w = httptest.NewRecorder()
	resetHandler(w, req)
	require.Equal(t, http.StatusConflict, w.Code)

	// 5. Degraded mode allows reset without force=true
	isResetRequested.Store(false)
	isDegraded = true
	degradedReason = "database disk image is malformed"
	defer func() {
		isDegraded = false
		degradedReason = ""
	}()

	req = httptest.NewRequest(http.MethodPost, "/reset", nil)
	w = httptest.NewRecorder()
	resetHandler(w, req)
	require.Equal(t, http.StatusAccepted, w.Code)
	waitStop()
}

func TestCleanRuntimeData(t *testing.T) {
	tempBase := t.TempDir()
	dataDir := filepath.Join(tempBase, "data")
	confDir := filepath.Join(tempBase, "etc")
	require.NoError(t, os.MkdirAll(dataDir, 0o755))
	require.NoError(t, os.MkdirAll(confDir, 0o755))

	// Create mock files in dataDir
	dbFile := filepath.Join(dataDir, "sqliteKV.db")
	require.NoError(t, os.WriteFile(dbFile, []byte("main db"), 0o600))
	walFile := filepath.Join(dataDir, "sqliteKV.db-wal")
	require.NoError(t, os.WriteFile(walFile, []byte("wal"), 0o600))
	cacheFile := filepath.Join(dataDir, "cache.db")
	require.NoError(t, os.WriteFile(cacheFile, []byte("cache db"), 0o600))
	dataInitMarker := filepath.Join(dataDir, "initialized123456")
	require.NoError(t, os.WriteFile(dataInitMarker, []byte(""), 0o600))

	// Create uploads and connections directories
	uploadsDir := filepath.Join(dataDir, "uploads")
	require.NoError(t, os.MkdirAll(uploadsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(uploadsDir, "test.zip"), []byte("plugin"), 0o600))
	connectionsDir := filepath.Join(dataDir, "connections")
	require.NoError(t, os.MkdirAll(connectionsDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(connectionsDir, "conn.yaml"), []byte("conn"), 0o600))

	// Create init.json in dataDir - MUST BE PRESERVED
	dataInitJson := filepath.Join(dataDir, "init.json")
	require.NoError(t, os.WriteFile(dataInitJson, []byte(`{"streams":{}}`), 0o600))

	// Create mock files in confDir
	confInitMarker := filepath.Join(confDir, "initialized123456")
	require.NoError(t, os.WriteFile(confInitMarker, []byte(""), 0o600))
	confInitJson := filepath.Join(confDir, "init.json")
	require.NoError(t, os.WriteFile(confInitJson, []byte(`{"rules":{}}`), 0o600))
	kuiperYaml := filepath.Join(confDir, "kuiper.yaml")
	require.NoError(t, os.WriteFile(kuiperYaml, []byte("basic: {}"), 0o600))

	// Execute clean
	err := cleanRuntimeDataLoc(dataDir, confDir)
	require.NoError(t, err)

	// Verify dataDir runtime data is wiped
	_, err = os.Stat(dbFile)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(walFile)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(cacheFile)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(dataInitMarker)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(uploadsDir)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(connectionsDir)
	require.True(t, os.IsNotExist(err))

	// Verify init.json in dataDir is PRESERVED
	content, err := os.ReadFile(dataInitJson)
	require.NoError(t, err)
	require.Equal(t, `{"streams":{}}`, string(content))

	// Verify confDir: initialized marker deleted, config & init.json PRESERVED
	_, err = os.Stat(confInitMarker)
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(confInitJson)
	require.NoError(t, err)
	_, err = os.Stat(kuiperYaml)
	require.NoError(t, err)

	// Verify unsafe directory rejection
	require.Error(t, cleanRuntimeDataLoc("/", confDir))
	require.Error(t, cleanRuntimeDataLoc("/etc", confDir))
	require.Error(t, cleanRuntimeDataLoc("", confDir))
	require.Error(t, cleanRuntimeDataLoc(".", confDir))
	require.Error(t, cleanRuntimeDataLoc("..", confDir))

	// Verify symlink safety: symlink pointing to etc directory is rejected
	symlinkToEtc := filepath.Join(tempBase, "symlink_etc")
	require.NoError(t, os.Symlink(confDir, symlinkToEtc))
	require.Error(t, cleanRuntimeDataLoc(symlinkToEtc, confDir), "symlink to etc must be rejected as unsafe")

	// Verify symlink pointing to system dir is rejected
	symlinkToRoot := filepath.Join(tempBase, "symlink_root")
	require.NoError(t, os.Symlink("/", symlinkToRoot))
	require.Error(t, cleanRuntimeDataLoc(symlinkToRoot, confDir), "symlink to root must be rejected as unsafe")

	// Verify plugins directory outside dataDir is untouched
	pluginsDir := filepath.Join(tempBase, "plugins")
	require.NoError(t, os.MkdirAll(pluginsDir, 0o755))
	pluginFile := filepath.Join(pluginsDir, "my_plugin.so")
	require.NoError(t, os.WriteFile(pluginFile, []byte("binary"), 0o600))
	require.NoError(t, cleanRuntimeDataLoc(dataDir, confDir))
	_, err = os.Stat(pluginFile)
	require.NoError(t, err, "plugins directory must remain untouched")

	// Verify data directory overlapping with plugins is rejected
	symlinkToPlugins := filepath.Join(tempBase, "symlink_plugins")
	require.NoError(t, os.Symlink(pluginsDir, symlinkToPlugins))
	require.False(t, isSafeDataDir(symlinkToPlugins, pluginsDir), "data overlapping with plugins must be unsafe")

	// Test relative paths
	relSubDir := filepath.Join(tempBase, "rel_test")
	relData := filepath.Join(relSubDir, "data")
	relEtc := filepath.Join(relSubDir, "etc")
	require.NoError(t, os.MkdirAll(relData, 0o755))
	require.NoError(t, os.MkdirAll(relEtc, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(relData, "junk.db"), []byte("data"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(relData, "init.json"), []byte("{}"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(relEtc, "initialized999"), []byte(""), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(relEtc, "kuiper.yaml"), []byte("kuiper"), 0o600))

	origWd, _ := os.Getwd()
	require.NoError(t, os.Chdir(relSubDir))
	defer func() {
		_ = os.Chdir(origWd)
	}()

	require.NoError(t, cleanRuntimeDataLoc("data", "etc"))
	_, err = os.Stat(filepath.Join(relData, "junk.db"))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(relData, "init.json"))
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(relEtc, "initialized999"))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(filepath.Join(relEtc, "kuiper.yaml"))
	require.NoError(t, err)
}

func TestStopEKuiper_BufferedSignal(t *testing.T) {
	origSignal := stopSignal
	stopSignal = make(chan struct{}, 1)
	defer func() { stopSignal = origSignal }()

	// 1. Send signal when no receiver is waiting
	stopEKuiper()

	// 2. Calling it a second time merges/drops gracefully without blocking
	stopEKuiper()

	// 3. Later, receiver should receive the signal
	select {
	case <-stopSignal:
		// success
	default:
		t.Fatal("expected stop signal to be buffered and received")
	}

	// 4. Channel should now be empty
	select {
	case <-stopSignal:
		t.Fatal("expected stop signal channel to have only 1 buffered signal")
	default:
		// success
	}
}

func TestDegradedMiddleware(t *testing.T) {
	isResetRequested.Store(false)
	defer isResetRequested.Store(false)

	isDegraded = true
	degradedReason = "database corrupt"
	defer func() {
		isDegraded = false
		degradedReason = ""
	}()

	r := mux.NewRouter()
	r.Use(degradedMiddleware)

	r.HandleFunc("/", rootHandler).Methods(http.MethodGet)
	r.HandleFunc("/ping", pingHandler).Methods(http.MethodGet)
	r.HandleFunc("/stop", stopHandler).Methods(http.MethodPost)
	r.HandleFunc("/reset", resetHandler).Methods(http.MethodPost)
	r.HandleFunc("/rules", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("rules ok"))
	}).Methods(http.MethodGet)

	// 1. /ping should be allowed
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// 2. / should return degraded status and reason
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var info information
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &info))
	require.True(t, info.Degraded)
	require.Equal(t, "degraded", info.Status)
	require.Equal(t, "database corrupt", info.DegradedReason)
	require.Equal(t, "database corrupt", info.Reason)

	// 3. /rules should be blocked with 503
	req = httptest.NewRequest(http.MethodGet, "/rules", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	var errResp map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
	require.Contains(t, errResp["error"], "degraded mode")

	// 4. When IsResetting() is true, even non-degraded requests to business endpoints should be blocked with 503
	isDegraded = false
	RequestReset()
	req = httptest.NewRequest(http.MethodGet, "/rules", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &errResp))
	require.Contains(t, errResp["error"], "factory reset")

	// 5. When IsResetting() is true, duplicate /reset request should pass through middleware and return 409 Conflict
	req = httptest.NewRequest(http.MethodPost, "/reset?force=true", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestUploadsStoreFailureDoesNotPanic(t *testing.T) {
	tempUploadsDb := uploadsDb
	tempUploadDir := uploadDir
	uploadsDb = nil
	uploadDir = t.TempDir()
	defer func() {
		uploadsDb = tempUploadsDb
		uploadDir = tempUploadDir
	}()

	err := upload(&fileContent{Name: "test.json", Content: "hello"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "uploads storage not initialized")
}
