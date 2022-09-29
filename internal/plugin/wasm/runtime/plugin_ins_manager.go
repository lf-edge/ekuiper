package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"os"
	"sync"
)

var (
	once sync.Once
	pm   *pluginInsManager
)

var WasmConf = &WasmConfig{
	SendTimeout: 1000,
}

type PluginIns struct {
	process      *os.Process
	runningCount int
	name         string
}

func (i *PluginIns) StartSymbol(ctx api.StreamContext, ctrl *Control) error {
	arg, err := json.Marshal(ctrl)
	if err != nil {
		fmt.Println("[plugin][wasm][runtime][plugin_ins_manager.go][StartSymbol] json.Marshal(1) err: ", err)
		return err
	}
	c := Command{
		Cmd: CMD_START,
		Arg: string(arg),
	}
	jsonArg, err := json.Marshal(c)
	if err != nil {
		fmt.Println("[plugin][wasm][runtime][plugin_ins_manager.go] json.Marshal(2) err: ", err)
		return err
	}
	fmt.Println("[plugin][wasm][runtime][plugin_ins_manager.go] (string)jsonArg: ", string(jsonArg))
	return err
}

func (i *PluginIns) StopSymbol(ctx api.StreamContext, ctrl *Control) error {
	arg, err := json.Marshal(ctrl)
	if err != nil {
		return err
	}
	c := Command{
		Cmd: CMD_STOP,
		Arg: string(arg),
	}
	jsonArg, err := json.Marshal(c)
	if err != nil {
		return err
	}
	fmt.Println("[plugin][wasm][runtime][plugin_ins_manager.go][StopSymbol] (string)jsonArg: ", string(jsonArg))
	return err
}

func (i *PluginIns) Stop() error {
	var err error
	if i.process != nil { // will also trigger process exit clean up
		err = i.process.Kill()
	}
	return err
}

// Manager plugin process and control socket
type pluginInsManager struct {
	instances map[string]*PluginIns
	sync.RWMutex
}

func (p *pluginInsManager) getPluginIns(name string) (*PluginIns, bool) {
	p.RLock()
	defer p.RUnlock()
	ins, ok := p.instances[name]
	return ins, ok
}

func (p *pluginInsManager) deletePluginIns(name string) {
	p.Lock()
	defer p.Unlock()
	delete(p.instances, name)
}

// AddPluginIns For mock only
func (p *pluginInsManager) AddPluginIns(name string, ins *PluginIns) {
	p.Lock()
	defer p.Unlock()
	p.instances[name] = ins
}

func (p *pluginInsManager) Kill(name string) error {
	p.Lock()
	defer p.Unlock()
	var err error
	if ins, ok := p.instances[name]; ok {
		err = ins.Stop()
		delete(p.instances, name)
	} else {
		return fmt.Errorf("instance %s not found", name)
	}
	return err
}

type PluginMeta struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Language string `json:"language"`
	//Executable string `json:"executable"`
	WasmFile   string `json:"wasmFile"`
	WasmEngine string `json:"wasmEngine"`
}
