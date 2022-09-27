// Copyright 2021 EMQ Technologies Co., Ltd.
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
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/portable/runtime"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"testing"
)

// Test only install API. Install from file is tested in the integration test in test/portable_rule_test

func init() {
	InitManager()
}

func TestManager_Install(t *testing.T) {
	s := httptest.NewServer(
		http.FileServer(http.Dir("../testzips")),
	)
	defer s.Close()
	endpoint := s.URL

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
		},
		{ // 1
			n:   "zipMissJson",
			u:   endpoint + "/functions/misc.zip",
			err: errors.New("fail to install plugin: missing or invalid json file zipMissJson.json"),
		},
		{ // 2
			n:   "urlerror",
			u:   endpoint + "/sinks/nozip",
			err: errors.New("invalid uri " + endpoint + "/sinks/nozip"),
		}, { // 3
			n:   "wrong",
			u:   endpoint + "/portables/wrong.zip",
			err: errors.New("fail to install plugin: missing mirror.exe"),
		}, { // 4
			n:   "wrongname",
			u:   endpoint + "/portables/mirror2.zip",
			err: errors.New("fail to install plugin: missing or invalid json file wrongname.json"),
		},
		{ // 5
			n: "mirror2",
			u: endpoint + "/portables/mirror2.zip",
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for i, tt := range data {
		p := &plugin.IOPlugin{
			Name: tt.n,
			File: tt.u,
		}
		err := manager.Register(p)
		if !reflect.DeepEqual(tt.err, err) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		} else if tt.err == nil {
			err := checkFileForMirror(manager.pluginDir, manager.etcDir, true)
			if err != nil {
				t.Errorf("%d: error : %s\n\n", i, err)
			}
		}
	}
}

func TestManager_Read(t *testing.T) {
	expPlugins := []*PluginInfo{
		{
			PluginMeta: runtime.PluginMeta{
				Name:       "mirror2",
				Version:    "v1.0.0",
				Language:   "go",
				Executable: filepath.Clean(path.Join(manager.pluginDir, "mirror2", "mirror2")),
			},
			Sources:   []string{"randomGo"},
			Sinks:     []string{"fileGo"},
			Functions: []string{"echoGo"},
		},
	}
	result := manager.List()
	if len(result) != 3 {
		t.Errorf("list result mismatch:\n  exp=%v\n  got=%v\n\n", expPlugins, result)
	}

	_, ok := manager.GetPluginInfo("mirror3")
	if ok {
		t.Error("find inexist plugin mirror3")
	}
	pi, ok := manager.GetPluginInfo("mirror2")
	if !ok {
		t.Error("can't find plugin mirror2")
	}
	if !reflect.DeepEqual(expPlugins[0], pi) {
		t.Errorf("Get plugin mirror2 mismatch:\n exp=%v\n got=%v", expPlugins[0], pi)
	}
	_, ok = manager.GetPluginMeta(plugin.SOURCE, "echoGo")
	if ok {
		t.Error("find inexist source symbol echo")
	}
	m, ok := manager.GetPluginMeta(plugin.SINK, "fileGo")
	if !ok {
		t.Error("can't find sink symbol fileGo")
	}
	if !reflect.DeepEqual(&(expPlugins[0].PluginMeta), m) {
		t.Errorf("Get sink symbol mismatch:\n exp=%v\n got=%v", expPlugins[0].PluginMeta, m)
	}
}

// This will start channel, so test it in integration tests.
//func TestFactory(t *testing.T){
//	_, err := manager.Source("alss")
//	expErr := fmt.Errorf("can't find random")
//	if !reflect.DeepEqual(expErr, err){
//		t.Errorf("error mismatch:\n  exp=%s\n  got=%s\n\n", expErr, err)
//	}
//	src, _ := manager.Source("randomGo")
//	if src  == nil {
//		t.Errorf("can't get source randomGo")
//	}
//	snk, _ := manager.Sink("fileGo")
//	if snk == nil {
//		t.Errorf("can't get sink fileGo")
//	}
//	fun, _ := manager.Function("echoGo")
//	if fun == nil {
//		t.Errorf("can't get function echoGo")
//	}
//	ok := manager.HasFunctionSet("echoGo")
//	if !ok {
//		t.Errorf("can't check function set")
//	}
//}

func TestDelete(t *testing.T) {
	err := manager.Delete("mirror2")
	if err != nil {
		t.Errorf("delete plugin error: %v", err)
	}
	err = checkFileForMirror(manager.pluginDir, manager.etcDir, false)
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
