// Copyright 2021-2022 EMQ Technologies Co., Ltd.
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

package wasm

import (
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"reflect"
	"testing"
)

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
		{ // 1
			n: "fibonacci",
			u: endpoint + "/wasm/fibonacci.zip",
		}, { // 2
			n:   "wrong",
			u:   endpoint + "/wasm/fibonacci.zip",
			err: errors.New("fail to install plugin: missing or invalid json file wrong.json"),
		}, { // 3
			n:   "test",
			u:   endpoint + "/wasm/add.zip",
			err: errors.New("fail to install plugin: missing or invalid json file test.json"),
		}, { // 4
			n: "ride",
			u: endpoint + "/wasm/ride.zip",
			//err: errors.New("fail to install plugin: missing or invalid wasm file"),
			//err: errors.New("invalid name ride: duplicate"),
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for i, tt := range data {
		p := &plugin.IOPlugin{
			Name: tt.n,
			File: tt.u,
		}
		//fmt.Println("------------")
		//fmt.Println("i: ", i)
		err := manager.Register(p)
		//fmt.Println("err :", err)
		if !reflect.DeepEqual(tt.err, err) {
			t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		} else {
			err := checkFileForMirror(manager.pluginDir, true)
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
				Name:       "fibonacci",
				Version:    "v1.0.0",
				WasmFile:   "/home/erfenjiao/ekuiper/plugins/wasm/fibonacci/fibonacci.wasm",
				WasmEngine: "wasmedge",
			},
			Functions: []string{"fib"},
		},
	}
	fmt.Println("[TestManager_Read] List: ")
	result := manager.List()
	fmt.Println("[TestManager_Read] result: ", result)
	pi, ok := manager.GetPluginInfo("fibonacci")
	if !ok {
		t.Error("can't find plugin fibonacci")
	}
	fmt.Println("[TestManager_Read] pi: ", pi)
	fmt.Println("[TestManager_Read] expPlugins[0]: ", expPlugins[0])
	if !reflect.DeepEqual(expPlugins[0], pi) {
		t.Errorf("Get plugin fibonacci mismatch:\n exp=%v\n got=%v", expPlugins[0], pi)
	}
}

func TestDelete(t *testing.T) {
	err := manager.Delete("fibonacci")
	if err != nil {
		t.Errorf("delete plugin error: %v", err)
	}
	err = manager.Delete("test")
	if err != nil {
		t.Errorf("delete plugin error: %v", err)
	}
	err = manager.Delete("ride")
	if err != nil {
		t.Errorf("delete plugin error: %v", err)
	}
}

func checkFileForMirror(pluginDir string, exist bool) error {
	requiredFiles := []string{
		path.Join(pluginDir, "fibonacci", "fibonacci.wasm"),
		path.Join(pluginDir, "fibonacci", "fibonacci.json"),
		//path.Join(etcDir, "sources", "randomGo.yaml"),
		//path.Join(etcDir, "sources", "randomGo.json"),
		//path.Join(etcDir, "functions", "echoGo.json"),
		//path.Join(etcDir, "sinks", "fileGo.json"),
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
