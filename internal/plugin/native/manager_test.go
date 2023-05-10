// Copyright 2021-2023 EMQ Technologies Co., Ltd.
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

	"github.com/lf-edge/ekuiper/internal/binder"
	"github.com/lf-edge/ekuiper/internal/binder/function"
	"github.com/lf-edge/ekuiper/internal/meta"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/testx"
)

func init() {
	testx.InitEnv()
	meta.InitYamlConfigManager()
	nativeManager, err := InitManager()
	if err != nil {
		panic(err)
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
		}, {
			t:   plugin.SOURCE,
			n:   "zipMissConf",
			u:   endpoint + "/sources/zipMissConf.zip",
			err: errors.New("fail to install plugin: invalid zip file: so file or conf file is missing"),
		}, {
			t:   plugin.SINK,
			n:   "urlerror",
			u:   endpoint + "/sinks/nozip",
			err: errors.New("invalid uri " + endpoint + "/sinks/nozip"),
		}, {
			t:   plugin.SINK,
			n:   "zipWrongname",
			u:   endpoint + "/sinks/zipWrongName.zip",
			err: errors.New("fail to install plugin: invalid zip file: so file or conf file is missing"),
		}, {
			t:   plugin.FUNCTION,
			n:   "zipMissSo",
			u:   endpoint + "/functions/zipMissSo.zip",
			err: errors.New("fail to install plugin: invalid zip file: so file or conf file is missing"),
		}, {
			t: plugin.SOURCE,
			n: "random2",
			u: endpoint + "/sources/random2.zip",
		}, {
			t: plugin.SOURCE,
			n: "random3",
			u: endpoint + "/sources/random3.zip",
			v: "1.0.0",
		}, {
			t:       plugin.SINK,
			n:       "file2",
			u:       endpoint + "/sinks/file2.zip",
			lowerSo: true,
		}, {
			t: plugin.FUNCTION,
			n: "echo2",
			u: endpoint + "/functions/echo2.zip",
			f: []string{"echo2", "echo3"},
		}, {
			t:   plugin.FUNCTION,
			n:   "echo2",
			u:   endpoint + "/functions/echo2.zip",
			err: errors.New("invalid name echo2: duplicate"),
		}, {
			t:   plugin.FUNCTION,
			n:   "misc",
			u:   endpoint + "/functions/echo2.zip",
			f:   []string{"misc", "echo3"},
			err: errors.New("function name echo3 already exists"),
		}, {
			t: plugin.FUNCTION,
			n: "comp",
			u: endpoint + "/functions/comp.zip",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for i, tt := range data {
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
		if !reflect.DeepEqual(tt.err, err) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		} else if tt.err == nil {
			err := checkFile(manager.pluginDir, manager.pluginConfDir, tt.t, tt.n, tt.v, tt.lowerSo)
			if err != nil {
				t.Errorf("%d: error : %s\n\n", i, err)
			}
		}
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
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data))

	for i, p := range data {
		result, ok := manager.GetPluginInfo(p.t, p.n)
		if !ok {
			t.Errorf("%d: get error : not found\n\n", i)
			return
		}
		if !reflect.DeepEqual(p.r, result) {
			t.Errorf("%d: result mismatch:\n  exp=%v\n  got=%v\n\n", i, p.r, result)
		}
	}
}

func TestManager_Delete(t *testing.T) {
	data := []struct {
		t   plugin.PluginType
		n   string
		err error
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
		},
	}
	fmt.Printf("The test bucket size is %d.\n\n", len(data))

	for i, p := range data {
		err := manager.Delete(p.t, p.n, false)
		if err != nil {
			t.Errorf("%d: delete error : %s\n\n", i, err)
		}
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
