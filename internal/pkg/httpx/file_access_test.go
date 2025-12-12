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

package httpx

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/model"
)

func TestReadFileAccessRestriction(t *testing.T) {
	// Create temp directory structure
	tempDir := t.TempDir()

	// Set up required directory structure
	dataDir := filepath.Join(tempDir, "data", "file_access_test")
	uploadsDir := filepath.Join(dataDir, "uploads")
	err := os.MkdirAll(uploadsDir, 0o755)
	require.NoError(t, err, "Should be able to create uploads dir")

	// Save original settings
	origConfig := conf.Config
	origTesting := conf.IsTesting
	origTestId := conf.TestId
	origEnv := os.Getenv(conf.KuiperBaseKey)

	// Set up KuiperBaseKey to point to our temp directory
	os.Setenv(conf.KuiperBaseKey, tempDir)
	conf.IsTesting = true
	conf.TestId = "file_access_test"

	// Cleanup after test
	defer func() {
		conf.Config = origConfig
		conf.IsTesting = origTesting
		conf.TestId = origTestId
		if origEnv == "" {
			os.Unsetenv(conf.KuiperBaseKey)
		} else {
			os.Setenv(conf.KuiperBaseKey, origEnv)
		}
	}()

	// Verify the data directory is correct
	gotDataDir, err := conf.GetDataLoc()
	require.NoError(t, err, "Should be able to get data location")
	require.Equal(t, dataDir, gotDataDir, "Data directory should match")

	// Create test file in uploads
	testFile := filepath.Join(uploadsDir, "allowed.txt")
	err = os.WriteFile(testFile, []byte("allowed content"), 0o644)
	require.NoError(t, err)

	// Create test file outside uploads (in data dir directly)
	outsideFile := filepath.Join(dataDir, "secret.txt")
	err = os.WriteFile(outsideFile, []byte("secret content"), 0o644)
	require.NoError(t, err)

	t.Run("AllowExternalFileAccess=false blocks outside path", func(t *testing.T) {
		conf.Config = &model.KuiperConf{}
		conf.Config.Basic.AllowExternalFileAccess = false

		_, err := ReadFile("file://" + outsideFile)
		assert.Error(t, err, "Should block access to files outside uploads")
		if err != nil {
			assert.Contains(t, err.Error(), "file access denied")
		}
	})

	t.Run("AllowExternalFileAccess=false allows uploads path", func(t *testing.T) {
		conf.Config = &model.KuiperConf{}
		conf.Config.Basic.AllowExternalFileAccess = false

		rc, err := ReadFile("file://" + testFile)
		assert.NoError(t, err, "Should allow access to files in uploads")
		if rc != nil {
			rc.Close()
		}
	})

	t.Run("AllowExternalFileAccess=false blocks path traversal", func(t *testing.T) {
		conf.Config = &model.KuiperConf{}
		conf.Config.Basic.AllowExternalFileAccess = false

		// Try path traversal
		traversalPath := filepath.Join(uploadsDir, "..", "secret.txt")
		_, err := ReadFile("file://" + traversalPath)
		assert.Error(t, err, "Should block path traversal attempts")
	})

	t.Run("AllowExternalFileAccess=true allows any path", func(t *testing.T) {
		conf.Config = &model.KuiperConf{}
		conf.Config.Basic.AllowExternalFileAccess = true

		rc, err := ReadFile("file://" + outsideFile)
		assert.NoError(t, err, "Should allow access when AllowExternalFileAccess is true")
		if rc != nil {
			rc.Close()
		}
	})

	t.Run("nil config restricts access", func(t *testing.T) {
		conf.Config = nil

		_, err := ReadFile("file://" + outsideFile)
		assert.Error(t, err, "Should block access when config is nil")
	})
}

func TestDownloadFile(t *testing.T) {
	tempDir := t.TempDir()
	sourceFile := filepath.Join(tempDir, "source.txt")
	err := os.WriteFile(sourceFile, []byte("content"), 0o644)
	require.NoError(t, err)

	targetDir := filepath.Join(tempDir, "target")
	err = os.Mkdir(targetDir, 0o755)
	require.NoError(t, err)

	// Since DownloadFile calls ReadFile, we need to set up the config to allow access
	// or use a path that ReadFile allows.
	// We'll set AllowExternalFileAccess to true for simplicity in this test
	origConfig := conf.Config
	defer func() { conf.Config = origConfig }()

	conf.Config = &model.KuiperConf{}
	conf.Config.Basic.AllowExternalFileAccess = true

	t.Run("DownloadFile success", func(t *testing.T) {
		downloaded, err := DownloadFile(targetDir, "downloaded.txt", "file://"+sourceFile)
		assert.NoError(t, err)
		assert.Equal(t, filepath.Join(targetDir, "downloaded.txt"), downloaded)

		content, err := os.ReadFile(downloaded)
		assert.NoError(t, err)
		assert.Equal(t, "content", string(content))
	})

	t.Run("DownloadFile fails with non-existent source", func(t *testing.T) {
		_, err := DownloadFile(targetDir, "fail.txt", "file://"+sourceFile+".missing")
		assert.Error(t, err)
	})
}
