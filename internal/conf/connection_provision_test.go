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

package conf

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestComputeConnectionYamlHash(t *testing.T) {
	require.NoError(t, ClearKVStorage())
	t.Cleanup(func() { _ = ClearKVStorage() })

	baseDir := t.TempDir()
	t.Setenv(KuiperBaseKey, baseDir)
	require.NoError(t, os.MkdirAll(filepath.Join(baseDir, "etc", "connections"), os.ModePerm))

	confDir, err := GetConfLoc()
	require.NoError(t, err)
	yamlPath := filepath.Join(confDir, "connections", "connection.yaml")

	t.Run("missing file returns stable empty hash", func(t *testing.T) {
		h1, err := ComputeConnectionYamlHash()
		require.NoError(t, err)
		h2, err := ComputeConnectionYamlHash()
		require.NoError(t, err)
		require.Equal(t, h1, h2)
		require.NotEmpty(t, h1)
	})

	t.Run("same content returns same hash", func(t *testing.T) {
		content := "mqtt:\n  cloud:\n    server: tcp://127.0.0.1:1883\n"
		require.NoError(t, os.WriteFile(yamlPath, []byte(content), 0o644))
		h1, err := ComputeConnectionYamlHash()
		require.NoError(t, err)
		h2, err := ComputeConnectionYamlHash()
		require.NoError(t, err)
		require.Equal(t, h1, h2)
	})

	t.Run("different content returns different hash", func(t *testing.T) {
		require.NoError(t, os.WriteFile(yamlPath, []byte("mqtt:\n  cloud:\n    server: tcp://127.0.0.1:1883\n"), 0o644))
		h1, err := ComputeConnectionYamlHash()
		require.NoError(t, err)

		require.NoError(t, os.WriteFile(yamlPath, []byte("mqtt:\n  cloud:\n    server: tcp://10.0.0.1:1883\n"), 0o644))
		h2, err := ComputeConnectionYamlHash()
		require.NoError(t, err)
		require.NotEqual(t, h1, h2)
	})

	t.Run("env override changes hash", func(t *testing.T) {
		require.NoError(t, os.WriteFile(yamlPath, []byte("mqtt:\n  cloud:\n    server: tcp://127.0.0.1:1883\n"), 0o644))
		h1, err := ComputeConnectionYamlHash()
		require.NoError(t, err)

		t.Setenv("CONNECTION__MQTT__CLOUD__SERVER", "tcp://env-override:1883")
		SetupEnv()
		h2, err := ComputeConnectionYamlHash()
		require.NoError(t, err)
		require.NotEqual(t, h1, h2)
	})
}

func TestConnectionYamlHashStorage(t *testing.T) {
	require.NoError(t, ClearKVStorage())
	t.Cleanup(func() { _ = ClearKVStorage() })

	t.Run("get returns empty when not set", func(t *testing.T) {
		require.Equal(t, "", GetConnectionYamlHash())
	})

	t.Run("set and get", func(t *testing.T) {
		require.NoError(t, SetConnectionYamlHash("abc123"))
		require.Equal(t, "abc123", GetConnectionYamlHash())
	})

	t.Run("overwrite", func(t *testing.T) {
		require.NoError(t, SetConnectionYamlHash("first"))
		require.NoError(t, SetConnectionYamlHash("second"))
		require.Equal(t, "second", GetConnectionYamlHash())
	})
}
