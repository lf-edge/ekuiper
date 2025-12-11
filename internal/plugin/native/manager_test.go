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

package native

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lf-edge/ekuiper/v2/internal/binder"
	"github.com/lf-edge/ekuiper/v2/internal/binder/function"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/meta"
	"github.com/lf-edge/ekuiper/v2/internal/plugin"
	"github.com/lf-edge/ekuiper/v2/internal/testx"
)

func init() {
	testx.InitEnv("native")
	conf.Config.Basic.EnablePrivateNet = true
	meta.InitYamlConfigManager()
	var (
		nativeManager *Manager
		err           error
	)
	for i := 0; i < 10; i++ {
		if nativeManager, err = InitManager(); err != nil {
			time.Sleep(10 * time.Millisecond)
		} else {
			break
		}
	}
	err = function.Initialize([]binder.FactoryEntry{{Name: "native plugin", Factory: nativeManager}})
	if err != nil {
		panic(err)
	}
}

func TestManager_Register(t *testing.T) {
	s := httptest.NewServer(
		http.FileServer(http.Dir("../testzips")),
	)
	defer s.Close()
	endpoint := s.URL
	pwd, err := os.Getwd()
	require.NoError(t, err)
	soPath := path.Join(pwd, "..", "..", "..", "plugins", "sources", "ZipMissConf.so")

	data := []struct {
		t       plugin.PluginType
		n       string
		u       string
		v       string
		f       []string
		lowerSo bool
		err     error
	}{
		{
			t:   plugin.SOURCE,
			n:   "",
			u:   "",
			err: errors.New("invalid name : should not be empty"),
		},
		{
			t:   plugin.SOURCE,
			n:   "zipMissConf",
			u:   endpoint + "/sources/zipMissConf.zip",
			err: fmt.Errorf("fail to install plugin: invalid zip file: expectFiles: 3, got filenames:[%s], zipFiles: [ZipMissConf.so], yamlFileChecked:false, soFileChecked:true", soPath),
		},
		{
			t:   plugin.SINK,
			n:   "urlerror",
			u:   endpoint + "/sinks/nozip",
			err: errors.New("invalid uri " + endpoint + "/sinks/nozip"),
		},
		{
			t:   plugin.SINK,
			n:   "zipWrongname",
			u:   endpoint + "/sinks/zipWrongName.zip",
			err: errors.New("fail to install plugin: invalid zip file: expectFiles: 1, got filenames:[], zipFiles: [Random2.so], yamlFileChecked:false, soFileChecked:false"),
		},
		{
			t:   plugin.FUNCTION,
			n:   "zipMissSo",
			u:   endpoint + "/functions/zipMissSo.zip",
			err: errors.New("fail to install plugin: invalid zip file: expectFiles: 1, got filenames:[], zipFiles: [zipMissSo.yaml], yamlFileChecked:false, soFileChecked:false"),
		},
		{
			t: plugin.SOURCE,
			n: "random2",
			u: endpoint + "/sources/random2.zip",
		},
		{
			t: plugin.SOURCE,
			n: "random3",
			u: endpoint + "/sources/random3.zip",
			v: "1.0.0",
		},
		{
			t:       plugin.SINK,
			n:       "file2",
			u:       endpoint + "/sinks/file2.zip",
			lowerSo: true,
		},
		{
			t: plugin.FUNCTION,
			n: "echo2",
			u: endpoint + "/functions/echo2.zip",
			f: []string{"echo2", "echo3"},
		},
		{
			t:   plugin.FUNCTION,
			n:   "echo2",
			u:   endpoint + "/functions/echo2.zip",
			err: errors.New("invalid name echo2: duplicate"),
		},
		{
			t:   plugin.FUNCTION,
			n:   "misc",
			u:   endpoint + "/functions/echo2.zip",
			f:   []string{"misc", "echo3"},
			err: errors.New("function name echo3 already exists"),
		},
		{
			t: plugin.FUNCTION,
			n: "comp",
			u: endpoint + "/functions/comp.zip",
		},
		{
			t:   plugin.SOURCE,
			n:   "invalidZip",
			u:   endpoint + "/sources/invalidZip.zip",
			err: errors.New("fail to install plugin: zip: not a valid zip file"),
		},
	}

	for i, tt := range data {
		t.Run(fmt.Sprintf("%d_%s", i, tt.n), func(t *testing.T) {
			var p plugin.Plugin
			if tt.t == plugin.FUNCTION {
				p = &plugin.FuncPlugin{
					IOPlugin: plugin.IOPlugin{
						Name: tt.n,
						File: tt.u,
					},
					Functions: tt.f,
				}
			} else {
				p = &plugin.IOPlugin{
					Name: tt.n,
					File: tt.u,
				}
			}
			err := manager.Register(tt.t, p)
			if tt.err == nil {
				assert.NoError(t, err)
				e := checkFile(manager.pluginDir, manager.pluginConfDir, tt.t, tt.n, tt.v, tt.lowerSo)
				assert.NoError(t, e)
			} else {
				assert.EqualError(t, err, tt.err.Error())
			}
		})
	}
}

func TestManager_List(t *testing.T) {
	data := []struct {
		t plugin.PluginType
		r []string
	}{
		{
			t: plugin.SOURCE,
			r: []string{"random", "random2", "random3"},
		}, {
			t: plugin.SINK,
			r: []string{"file2"},
		}, {
			t: plugin.FUNCTION,
			r: []string{"accumulateWordCount", "comp", "countPlusOne", "echo", "echo2"},
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data))

	for i, p := range data {
		result := manager.List(p.t)
		sort.Strings(result)
		if !reflect.DeepEqual(p.r, result) {
			t.Errorf("%d: result mismatch:\n  exp=%v\n  got=%v\n\n", i, p.r, result)
		}
	}
}

func TestManager_Symbols(t *testing.T) {
	r := []string{"accumulateWordCount", "comp", "countPlusOne", "echo", "echo2", "echo3", "misc"}
	result := manager.ListSymbols()
	sort.Strings(result)
	if !reflect.DeepEqual(r, result) {
		t.Errorf("result mismatch:\n  exp=%v\n  got=%v\n\n", r, result)
	}
	p, ok := manager.GetPluginBySymbol(plugin.FUNCTION, "echo3")
	if !ok {
		t.Errorf("cannot find echo3 symbol")
	}
	if p != "echo2" {
		t.Errorf("wrong plugin %s for echo3 symbol", p)
	}

	_, ok = manager.GetPluginVersionBySymbol(plugin.FUNCTION, "none")
	assert.False(t, ok)

	_, ok = manager.GetPluginVersionBySymbol(plugin.SINK, "none")
	assert.False(t, ok)
}

func TestManager_Desc(t *testing.T) {
	data := []struct {
		t plugin.PluginType
		n string
		r map[string]interface{}
	}{
		{
			t: plugin.SOURCE,
			n: "random2",
			r: map[string]interface{}{
				"name":    "random2",
				"version": "",
			},
		}, {
			t: plugin.SOURCE,
			n: "random3",
			r: map[string]interface{}{
				"name":    "random3",
				"version": "1.0.0",
			},
		}, {
			t: plugin.FUNCTION,
			n: "echo2",
			r: map[string]interface{}{
				"name":      "echo2",
				"version":   "",
				"functions": []string{"echo2", "echo3"},
			},
		}, {
			t: plugin.FUNCTION,
			n: "echo20",
			r: nil,
		},
	}

	for _, d := range data {
		t.Run(d.n, func(t *testing.T) {
			result, ok := manager.GetPluginInfo(d.t, d.n)
			if d.r == nil {
				assert.Nil(t, result)
				assert.False(t, ok)
			} else {
				assert.True(t, ok)
				assert.Equal(t, d.r, result)
			}
		})
	}
}

func TestManager_Delete(t *testing.T) {
	data := []struct {
		t   plugin.PluginType
		n   string
		err string
	}{
		{
			t: plugin.SOURCE,
			n: "random2",
		}, {
			t: plugin.SINK,
			n: "file2",
		}, {
			t: plugin.FUNCTION,
			n: "echo2",
		}, {
			t: plugin.SOURCE,
			n: "random3",
		}, {
			t: plugin.FUNCTION,
			n: "comp",
		}, {
			t:   plugin.FUNCTION,
			n:   "",
			err: "invalid name : should not be empty",
		},
	}
	for _, tt := range data {
		t.Run(tt.n, func(t *testing.T) {
			err := manager.Delete(tt.t, tt.n, false)
			if tt.err != "" {
				assert.EqualError(t, err, tt.err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func checkFile(pluginDir string, etcDir string, t plugin.PluginType, name string, version string, lowerSo bool) error {
	var soName string
	if !lowerSo {
		soName = ucFirst(name) + ".so"
		if version != "" {
			soName = fmt.Sprintf("%s@v%s.so", ucFirst(name), version)
		}
	} else {
		soName = name + ".so"
		if version != "" {
			soName = fmt.Sprintf("%s@v%s.so", name, version)
		}
	}

	soPath := path.Join(pluginDir, plugin.PluginTypes[t], soName)
	_, err := os.Stat(soPath)
	if err != nil {
		return err
	}
	if t == plugin.SOURCE {
		etcPath := path.Join(etcDir, plugin.PluginTypes[t], name+".yaml")
		_, err = os.Stat(etcPath)
		if err != nil {
			return err
		}
	}
	return nil
}
