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
	ctrlChan     ControlChannel
	runningCount int
	name         string
}

func NewPluginIns(name string, ctrlChan ControlChannel, process *os.Process) *PluginIns {
	// if process is not passed, it is run in simulator mode. Then do not count running.
	// so that it won't be automatically close.
	fmt.Println("[internal][plugin][wasm][runtime][plugin_ins_manager.go][NewPluginIns] start")
	rc := 0
	if process == nil {
		rc = 1
	}
	return &PluginIns{
		process:      process,
		ctrlChan:     ctrlChan,
		runningCount: rc,
		name:         name,
	}
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
	err = i.ctrlChan.SendCmd(jsonArg)
	if err == nil {
		i.runningCount++
		ctx.GetLogger().Infof("started symbol %s", ctrl.SymbolName)
	}
	fmt.Println("[plugin][wasm][runtime][plugin_ins_manager.go] SendCmd err: ", err)
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
	err = i.ctrlChan.SendCmd(jsonArg)
	i.runningCount--
	ctx.GetLogger().Infof("stopped symbol %s", ctrl.SymbolName)
	if i.runningCount == 0 {
		err := GetPluginInsManager().Kill(i.name)
		if err != nil {
			ctx.GetLogger().Infof("fail to stop plugin %s: %v", i.name, err)
			return err
		}
		ctx.GetLogger().Infof("stop plugin %s", i.name)
	}
	return err
}

func (i *PluginIns) Stop() error {
	var err error
	if i.ctrlChan != nil {
		err = i.ctrlChan.Close()
	}
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
	fmt.Println("[plugin][wasm][runtime][plugin_ins_manager.go] ins: ", ins)
	p.instances[name] = ins
}

func GetPluginInsManager() *pluginInsManager {
	once.Do(func() {
		pm = &pluginInsManager{
			instances: make(map[string]*PluginIns),
		}
	})
	return pm
}

//func (p *pluginInsManager) getOrStartProcess(pluginMeta *PluginMeta, pconf *WasmConfig) (*PluginIns, error) {
//	p.Lock()
//	defer p.Unlock()
//	if ins, ok := p.instances[pluginMeta.Name]; ok {
//		return ins, nil
//	}
//
//	conf.Log.Infof("create control channel")
//	ctrlChan, err := CreateControlChannel(pluginMeta.Name)
//	if err != nil {
//		return nil, fmt.Errorf("can't create new control channel: %s", err.Error())
//	}
//	defer func() {
//		if err != nil {
//			_ = ctrlChan.Close()
//		}
//	}()
//	conf.Log.Infof("executing plugin")
//	jsonArg, err := json.Marshal(pconf)
//	if err != nil {
//		return nil, fmt.Errorf("invalid conf: %v", pconf)
//	}
//	var cmd *exec.Cmd
//	switch pluginMeta.Language {
//	case "go":
//		conf.Log.Printf("starting go plugin executable %s", pluginMeta.Executable)
//		cmd = exec.Command(pluginMeta.Executable, string(jsonArg))
//	case "python":
//		conf.Log.Printf("starting python plugin executable %s with script %s\n", conf.Config.Portable.PythonBin, pluginMeta.Executable)
//		cmd = exec.Command(conf.Config.Portable.PythonBin, pluginMeta.Executable, string(jsonArg))
//	default:
//		return nil, fmt.Errorf("unsupported language: %s", pluginMeta.Language)
//	}
//	cmd.Stdout = conf.Log.Out
//	cmd.Stderr = conf.Log.Out
//
//	conf.Log.Println("plugin starting")
//	err = cmd.Start()
//	if err != nil {
//		// error
//		return nil, fmt.Errorf("plugin executable %s stops with error %v", pluginMeta.Executable, err)
//	}
//	process := cmd.Process
//	conf.Log.Printf("plugin started pid: %d\n", process.Pid)
//	defer func() {
//		if err != nil {
//			_ = process.Kill()
//		}
//	}()
//	go infra.SafeRun(func() error { // just print out error inside
//		err = cmd.Wait()
//		if err != nil {
//			conf.Log.Printf("plugin executable %s stops with error %v", pluginMeta.Executable, err)
//		}
//		// must make sure the plugin ins is not cleaned up yet by checking the process identity
//		if ins, ok := p.getPluginIns(pluginMeta.Name); ok && ins.process == cmd.Process {
//			if ins.ctrlChan != nil {
//				_ = ins.ctrlChan.Close()
//			}
//			p.deletePluginIns(pluginMeta.Name)
//		}
//		return nil
//	})
//
//	conf.Log.Println("waiting handshake")
//	err = ctrlChan.Handshake()
//	if err != nil {
//		return nil, fmt.Errorf("plugin %s control handshake error: %v", pluginMeta.Executable, err)
//	}
//	ins := NewPluginIns(pluginMeta.Name, ctrlChan, process)
//	p.instances[pluginMeta.Name] = ins
//	conf.Log.Println("plugin start running")
//	return ins, nil
//}

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
