// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package portable

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
)

// Test only install API. Install from file is tested in the integration test in test/portable_rule_test

func init() {
	testx.InitEnv("portable")
	// Wait for other db tests to finish to avoid db lock
	for i := 0; i < 10; i++ {
		if _, err := InitManager(); err != nil {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	meta.InitYamlConfigManager()
}

func TestManager_Install(t *testing.T) {
	s := httptest.NewServer(
		http.FileServer(http.Dir("../testzips")),
	)
	defer s.Close()
	endpoint := s.URL

	pwd, err := os.Getwd()
	require.NoError(t, err)
	ppath := path.Join(pwd, "..", "..", "..", "plugins", "portable", "mirror2", "mirror2")

	data := []struct {
		n   string
		u   string
		v   string
		err error
	}{
		{ // 0
			n:   "",
			u:   "",
			err: errors.New("invalid name : should not be empty"),
		}, { // 1
			n:   "zipMissJson",
			u:   endpoint + "/functions/misc.zip",
			err: errors.New("fail to install plugin: missing or invalid json file zipMissJson.json, found 1 files in total"),
		}, { // 2
			n:   "urlerror",
			u:   endpoint + "/sinks/nozip",
			err: errors.New("invalid uri " + endpoint + "/sinks/nozip"),
		}, { // 3
			n:   "wrong",
			u:   endpoint + "/portables/wrong.zip",
			err: errors.New("fail to install plugin: missing mirror.exe"),
		}, { // 4
			n:   "wrongname",
			u:   endpoint + "/portables/mirror.zip",
			err: errors.New("fail to install plugin: missing or invalid json file wrongname.json, found 9 files in total"),
		}, { // 5
			n:   "mirror2",
			u:   endpoint + "/portables/mirror.zip",
			err: fmt.Errorf("fail to install plugin: plugin executable %s stops with error fork/exec %s: permission denied", ppath, ppath),
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for _, tt := range data {
		p := &plugin.IOPlugin{
			Name: tt.n,
			File: tt.u,
		}
		m, err := InitManager()
		require.NoError(t, err)
		err = m.Register(p)
		if err != nil {
			require.Equal(t, tt.err, err)
		} else {
			err := checkFileForMirror(m.pluginDir, m.pluginDataDir, true)
			require.NoError(t, err)
		}
	}
}

func TestManager_Read(t *testing.T) {
	result := manager.List()
	if len(result) != 1 {
		t.Errorf("list result mismatch:\n got=%v\n\n", result)
	}

	_, ok := manager.GetPluginInfo("mirror3")
	assert.False(t, ok, "mirror3 should not be found")

	_, ok = manager.GetPluginInfo("mirror2")
	assert.False(t, ok, "plugin mirror2 should not be found")

	_, ok = manager.GetPluginMeta(plugin.SOURCE, "echoGo")
	assert.False(t, ok, "symbol echoGo should not be found")

	_, ok = manager.GetPluginMeta(plugin.SINK, "fileGo")
	assert.False(t, ok, "symbol fileGo should not be found")
}

func TestDelete(t *testing.T) {
	err := manager.Delete("mirror2")
	require.EqualError(t, err, "portable plugin mirror2 is not found")
	err = checkFileForMirror(manager.pluginDir, manager.pluginDataDir, false)
	if err != nil {
		t.Errorf("error : %s\n\n", err)
	}
}

func checkFileForMirror(pluginDir, etcDir string, exist bool) error {
	requiredFiles := []string{
		path.Join(pluginDir, "mirror2", "mirror2"),
		path.Join(pluginDir, "mirror2", "mirror2.json"),
		path.Join(etcDir, "sources", "randomGo.yaml"),
		path.Join(etcDir, "sources", "randomGo.json"),
		path.Join(etcDir, "functions", "echoGo.json"),
		path.Join(etcDir, "sinks", "fileGo.json"),
	}
	for _, file := range requiredFiles {
		_, err := os.Stat(file)
		if exist && err != nil {
			return err
		} else if !exist && err == nil {
			return fmt.Errorf("file still exists: %s", file)
		}
	}
	return nil
}

func TestManagerErr(t *testing.T) {
	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/conf/GetPluginsLocErr", "return(true)")
	_, err := InitManager()
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/conf/GetPluginsLocErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/conf/GetConfLocErr", "return(true)")
	_, err = InitManager()
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/conf/GetConfLocErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/syncRegistryErr", "return(true)")
	_, err = InitManager()
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/syncRegistryErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/plgDBErr", "return(true)")
	_, err = InitManager()
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/plgDBErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/plgStatusDbErr", "return(true)")
	_, err = InitManager()
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/plgStatusDbErr")

	m, err := InitManager()
	require.NoError(t, err)
	require.NotNil(t, m)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/syncRegistryReadDirErr", "return(true)")
	err = m.syncRegistry()
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/syncRegistryReadDirErr")

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/parsePluginJsonErr", "return(true)")
	err = m.parsePlugin("mock")
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/parsePluginJsonErr")

	err = m.doRegister("mock", &PluginInfo{}, true)
	require.Error(t, err)
}

func TestParsePluginJson(t *testing.T) {
	m, err := InitManager()
	require.NoError(t, err)
	require.NotNil(t, m)

	failpoint.Enable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/parsePluginJsonReadJsonUnmarshalErr", "return(true)")
	_, err = m.parsePluginJson("mock")
	require.Error(t, err)
	failpoint.Disable("github.com/lf-edge/ekuiper/v2/internal/plugin/portable/parsePluginJsonReadJsonUnmarshalErr")

	_, err = m.parsePluginJson("mock")
	require.Error(t, err)
	m.reg.Set("mirror", &PluginInfo{})
	_, err = m.parsePluginJson("mirror")
	require.Error(t, err)
}

func TestRegisterErr(t *testing.T) {
	s := httptest.NewServer(
		http.FileServer(http.Dir("../testzips")),
	)
	defer s.Close()
	endpoint := s.URL
	p := &plugin.IOPlugin{
		Name: "mirror2",
		File: endpoint + "/portables/mirror.zip",
	}

	manager.reg.Set("mirror2", &PluginInfo{})
	err := manager.Register(p)
	require.Error(t, err)
	manager.reg.Delete("mirror2")

	testcases := []struct {
		mockErr string
	}{
		{
			mockErr: "github.com/lf-edge/ekuiper/v2/internal/pkg/httpx/DownloadFileErr",
		},
		{
			mockErr: "github.com/lf-edge/ekuiper/v2/internal/plugin/portable/installOpenReaderErr",
		},
		{
			mockErr: "github.com/lf-edge/ekuiper/v2/internal/plugin/portable/installFileOpenErr",
		},
		{
			mockErr: "github.com/lf-edge/ekuiper/v2/internal/plugin/portable/installReadErr",
		},
		{
			mockErr: "github.com/lf-edge/ekuiper/v2/internal/plugin/portable/installJsonMarshalErr",
		},
		{
			mockErr: "github.com/lf-edge/ekuiper/v2/internal/plugin/portable/PluginInfoValidateErr",
		},
		{
			mockErr: "github.com/lf-edge/ekuiper/v2/internal/plugin/portable/PluginInfoValidateErr",
		},
		{
			mockErr: "github.com/lf-edge/ekuiper/v2/internal/plugin/portable/installMkdirErr",
		},
		{
			mockErr: "github.com/lf-edge/ekuiper/v2/internal/pkg/filex/UnzipToErr",
		},
	}

	for _, testcase := range testcases {
		failpoint.Enable(testcase.mockErr, "return(true)")
		err = manager.Register(p)
		require.Error(t, err, testcase.mockErr)
		failpoint.Disable(testcase.mockErr)
	}
}
