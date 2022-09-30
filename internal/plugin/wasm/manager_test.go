package wasm

import (
	"errors"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/plugin"
	"github.com/lf-edge/ekuiper/internal/plugin/wasm/runtime"
	"net/http"
	"net/http/httptest"
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
		{ // 0
			n:   "",
			u:   "",
			err: errors.New("invalid name : should not be empty"),
		}, { // 1
			n:   "fibonacci",
			u:   endpoint + "/wasm/fibonacci.zip",
			err: errors.New("fail to install plugin: missing or invalid json file fibonacci.json"),
		},
		{ // 2
			n:   "wrong",
			u:   endpoint + "/wasm/fibonacci.zip",
			err: errors.New("fail to install plugin: missing or invalid file name"),
		}, { // 3
			n:   "add",
			u:   endpoint + "/wasm/add.zip",
			err: errors.New("fail to install plugin: missing or invalid zip file"),
		}, { // 4
			n:   "ride",
			u:   endpoint + "/wasm/ride.zip",
			err: errors.New("fail to install plugin: missing or invalid zip file"),
		},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for i, tt := range data {
		p := &plugin.IOPlugin{
			Name: tt.n,
			File: tt.u,
		}
		fmt.Println("------------")
		fmt.Println("i: ", i)
		err := manager.Register(p)
		fmt.Println("err :", err)
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
	err = manager.Delete("add")
	if err != nil {
		t.Errorf("delete plugin error: %v", err)
	}
	err = manager.Delete("ride")
	if err != nil {
		t.Errorf("delete plugin error: %v", err)
	}
}
