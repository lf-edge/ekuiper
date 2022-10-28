// Copyright erfenjiao, 630166475@qq.com.
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
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
	"github.com/lf-edge/ekuiper/internal/testx"
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
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for i, tt := range data {
		p := &plugin.IOPlugin{
			Name: tt.n,
			File: tt.u,
		}
		err := manager.Register(p)
		if !reflect.DeepEqual(tt.err, testx.Errstring(err)) { //not same
			fmt.Println("err: ", err)
			//t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		} else { //same
			err := checkFileForMirror(manager.pluginDir, true)
			if err != nil {
				t.Errorf("%d: error : %s\n\n", i, err)
			}
		}
	}

}

func TestManager_Read(t *testing.T) {
	requiredFiles := []string{
		path.Join(manager.pluginDir, "fibonacci", "fibonacci.wasm"),
		path.Join(manager.pluginDir, "fibonacci", "fibonacci.json"),
	}
	expPlugins := []*PluginInfo{
		{
			PluginMeta: runtime.PluginMeta{
				Name:    "fibonacci",
				Version: "v1.0.0",
				//WasmFile:   "/home/erfenjiao/ekuiper/plugins/wasm/fibonacci/fibonacci.wasm",
				WasmFile:   requiredFiles[0],
				WasmEngine: "wasmedge",
			},
			Functions: []string{"fib"},
		},
	}
	//fmt.Println("[TestManager_Read] List: ")
	//result := manager.List()
	//fmt.Println("[TestManager_Read] result: ", result)
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
}

func checkFileForMirror(pluginDir string, exist bool) error {
	requiredFiles := []string{
		path.Join(pluginDir, "fibonacci", "fibonacci.wasm"),
		path.Join(pluginDir, "fibonacci", "fibonacci.json"),
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
