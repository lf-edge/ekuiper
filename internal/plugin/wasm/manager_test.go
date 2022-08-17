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

func TestInitManager(t *testing.T) {
	InitManager()
	//etcDir:  /home/erfenjiao/ekuiper/etc
	//pluginDir:  /home/erfenjiao/ekuiper/plugins/wasm
	checkFileForFib("/home/erfenjiao/ekuiper/plugins/wasm", "/home/erfenjiao/ekuiper/etc", true)
}

func checkFileForFib(pluginDir, etcDir string, exist bool) error {
	fmt.Println("[test][checkFileForFib] start: ")
	requiredFiles := []string{
		//path.Join(pluginDir, "fibonacci", "fibonacci"),
		path.Join(pluginDir, "fibonacci", "fibonacci.json"),
		path.Join(etcDir, "functions", "fibonacci.json"),
	}
	for _, file := range requiredFiles {
		_, err := os.Stat(file)
		fmt.Println("[test][checkFileForFib] file: ", file)
		//file:  /home/erfenjiao/ekuiper/plugins/wasm/fibonacci/fibonacci
		if exist && err != nil {
			return err
		} else if !exist && err == nil {
			return fmt.Errorf("file still exists: %s", file)
		}
	}
	return nil
}

func TestManager_Install(t *testing.T) {
	InitManager()
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
		//{ // 0
		//	n:   "",
		//	u:   "",
		//	err: errors.New("invalid name : should not be empty"),
		//},
		{ // 1
			n:   "fibonacci",
			u:   endpoint + "/wasm/fibonacci.zip",
			err: errors.New("fail to install plugin: missing or invalid json file fibonacci.json"),
		},
		//{ // 2
		//	n:   "wrong",
		//	u:   endpoint + "/wasm/fibonacci.zip",
		//	err: errors.New("fail to install plugin: missing mirror.exe"),
		//}, { // 3
		//	n:   "wrongname",
		//	u:   endpoint + "/wasm/fibonacci.zip",
		//	err: errors.New("fail to install plugin: missing or invalid json file wrongname.json"),
		//}, { // 4
		//	n: "fibonacci",
		//	u: endpoint + "/wasm/fibonacci.zip",
		//},
	}

	fmt.Printf("The test bucket size is %d.\n\n", len(data))
	for i, tt := range data {
		p := &plugin.IOPlugin{
			Name: tt.n,
			File: tt.u,
		}
		fmt.Println("------------")
		fmt.Println("i: ", i)
		fmt.Println("p: ", p)
		//m, err := wasm.MockManager(map[string]*wasm.PluginInfo{testingPlugin.Name: testingPlugin})
		//if err != nil {
		//	panic(err)
		//}
		fmt.Println("Register: ")
		err := manager.Register(p)
		fmt.Println("err :", err)
		//if !reflect.DeepEqual(tt.err, err) {
		//	t.Errorf("%d: error mismatch:\n  exp=%s\n  got=%s\n\n", i, tt.err, err)
		//} else if tt.err == nil {
		//	err := checkFileForFib(manager.pluginDir, manager.etcDir, true)
		//	if err != nil {
		//		t.Errorf("%d: error : %s\n\n", i, err)
		//	}
		//}
	}

}

func TestManager_Read(t *testing.T) {
	expPlugins := []*PluginInfo{
		{
			PluginMeta: runtime.PluginMeta{
				Name:     "fibonacci",
				Version:  "v1.0.0",
				Language: "go",
				//Executable: filepath.Clean(path.Join(manager.pluginDir, "mirror2", "mirror2")),
				WasmFile:   "plugins/wasm/fib/fibonacci.wasm",
				WasmEngine: "wasmedge",
			},
			Functions: []string{"fibonacci"},
		},
	}
	//fmt.Println("Executable: ", PluginInfo{PluginMeta: runtime.PluginMeta{Executable: filepath.Clean(path.Join(manager.pluginDir, "mirror2", "mirror2"))}})
	///home/erfenjiao/ekuiper/plugins/portable/mirror2/mirror2
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
