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

package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/infra"
	"os"
	"os/exec"
	"sync"
)

var (
	once sync.Once
	pm   *pluginInsManager
)

// TODO setting configuration
var PortbleConf = &PortableConfig{
	SendTimeout: 1000,
}

// PluginIns created at two scenarios
// 1. At runtime, plugin is created/updated: in order to be able to reload rules that already uses previous ins
// 2. At system start/restart, when plugin is used by a rule
// Once created, never deleted until delete plugin command or system shutdown
type PluginIns struct {
	sync.RWMutex
	name     string
	ctrlChan ControlChannel // the same lifecycle as pluginIns, once created keep listening
	// audit the commands, so that when restarting the plugin, we can replay the commands
	commands map[Meta][]byte
	process  *os.Process // created when used by rule and deleted when no rule uses it
}

func NewPluginIns(name string, ctrlChan ControlChannel, process *os.Process) *PluginIns {
	return &PluginIns{
		process:  process,
		ctrlChan: ctrlChan,
		name:     name,
		commands: make(map[Meta][]byte),
	}
}

func NewPluginInsForTest(name string, ctrlChan ControlChannel) *PluginIns {
	commands := make(map[Meta][]byte)
	commands[Meta{
		RuleId:     "test",
		OpId:       "test",
		InstanceId: 0,
	}] = []byte{}
	return &PluginIns{
		process:  nil,
		ctrlChan: ctrlChan,
		name:     name,
		commands: commands,
	}
}

func (i *PluginIns) StartSymbol(ctx api.StreamContext, ctrl *Control) error {
	arg, err := json.Marshal(ctrl)
	if err != nil {
		return err
	}
	c := Command{
		Cmd: CMD_START,
		Arg: string(arg),
	}
	jsonArg, err := json.Marshal(c)
	if err != nil {
		return err
	}
	err = i.ctrlChan.SendCmd(jsonArg)
	if err == nil {
		i.Lock()
		i.commands[ctrl.Meta] = jsonArg
		i.Unlock()
		ctx.GetLogger().Infof("started symbol %s", ctrl.SymbolName)
	}
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
	err = i.ctrlChan.SendCmd(jsonArg)
	if err == nil {
		referred := false
		i.Lock()
		delete(i.commands, ctrl.Meta)
		referred = len(i.commands) > 0
		i.Unlock()
		ctx.GetLogger().Infof("stopped symbol %s", ctrl.SymbolName)
		if !referred {
			err := GetPluginInsManager().Kill(i.name)
			if err != nil {
				ctx.GetLogger().Infof("fail to stop plugin %s: %v", i.name, err)
				return err
			}
			ctx.GetLogger().Infof("stop plugin %s", i.name)
		}
	}
	return err
}

// Stop intentionally
func (i *PluginIns) Stop() error {
	var err error
	i.RLock()
	defer i.RUnlock()
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

func GetPluginInsManager() *pluginInsManager {
	once.Do(func() {
		pm = &pluginInsManager{
			instances: make(map[string]*PluginIns),
		}
	})
	return pm
}

func (p *pluginInsManager) getPluginIns(name string) (*PluginIns, bool) {
	p.RLock()
	defer p.RUnlock()
	ins, ok := p.instances[name]
	return ins, ok
}

// deletePluginIns should only run when there is no state aka. commands
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

// CreateIns Run when plugin is created/updated
func (p *pluginInsManager) CreateIns(pluginMeta *PluginMeta) {
	p.Lock()
	defer p.Unlock()
	if ins, ok := p.instances[pluginMeta.Name]; ok {
		if len(ins.commands) != 0 {
			go p.getOrStartProcess(pluginMeta, PortbleConf, true)
		}
	}
}

// getOrStartProcess Control the plugin process lifecycle.
// Need to manage the resources: instances map, control socket, plugin process
// May be called at plugin creation or restart with previous state(ctrlCh, commands)
// PluginIns is created by plugin manager but started by rule/funcop.
// During plugin delete/update, if the commands is not empty, keep the ins for next creation and restore
// 1. During creation, clean up those resources for any errors in defer immediately after the resource is created.
// 2. During plugin running, when detecting plugin process exit, clean up those resources for the current ins.
func (p *pluginInsManager) getOrStartProcess(pluginMeta *PluginMeta, pconf *PortableConfig, pluginCreation bool) (*PluginIns, error) {
	p.Lock()
	defer p.Unlock()
	var (
		ins *PluginIns
		ok  bool
	)
	// run initialization for firstly creating plugin instance
	ins, ok = p.instances[pluginMeta.Name]
	if !ok {
		ins = NewPluginIns(pluginMeta.Name, nil, nil)
		p.instances[pluginMeta.Name] = ins
	}
	// ins process has not run yet
	if !pluginCreation && len(ins.commands) != 0 {
		return ins, nil
	}
	// should only happen for first start, then the ctrl channel will keep running
	if ins.ctrlChan == nil {
		conf.Log.Infof("create control channel")
		ctrlChan, err := CreateControlChannel(pluginMeta.Name)
		if err != nil {
			return nil, fmt.Errorf("can't create new control channel: %s", err.Error())
		}
		defer func() {
			if err != nil {
				_ = ctrlChan.Close()
			}
		}()
		ins.ctrlChan = ctrlChan
	}
	// init or restart all need to run the process
	conf.Log.Infof("executing plugin")
	jsonArg, err := json.Marshal(pconf)
	if err != nil {
		return nil, fmt.Errorf("invalid conf: %v", pconf)
	}
	var cmd *exec.Cmd
	err = infra.SafeRun(func() error {
		switch pluginMeta.Language {
		case "go":
			conf.Log.Printf("starting go plugin executable %s", pluginMeta.Executable)
			cmd = exec.Command(pluginMeta.Executable, string(jsonArg))

		case "python":
			conf.Log.Printf("starting python plugin executable %s with script %s\n", conf.Config.Portable.PythonBin, pluginMeta.Executable)
			cmd = exec.Command(conf.Config.Portable.PythonBin, pluginMeta.Executable, string(jsonArg))
		default:
			return fmt.Errorf("unsupported language: %s", pluginMeta.Language)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fail to start plugin %s: %v", pluginMeta.Name, err)
	}
	cmd.Stdout = conf.Log.Out
	cmd.Stderr = conf.Log.Out

	conf.Log.Println("plugin starting")
	err = cmd.Start()
	if err != nil {
		return nil, fmt.Errorf("plugin executable %s stops with error %v", pluginMeta.Executable, err)
	}
	process := cmd.Process
	conf.Log.Printf("plugin started pid: %d\n", process.Pid)
	defer func() {
		if err != nil {
			_ = process.Kill()
		}
	}()
	go infra.SafeRun(func() error { // just print out error inside
		err = cmd.Wait()
		if err != nil {
			conf.Log.Printf("plugin executable %s stops with error %v", pluginMeta.Executable, err)
		}
		// must make sure the plugin ins is not cleaned up yet by checking the process identity
		// clean up for stop unintentionally
		if ins, ok := p.getPluginIns(pluginMeta.Name); ok && ins.process == cmd.Process {
			ins.Lock()
			if len(ins.commands) == 0 {
				if ins.ctrlChan != nil {
					_ = ins.ctrlChan.Close()
				}
				p.deletePluginIns(pluginMeta.Name)
			}
			ins.process = nil
			ins.Unlock()
		}
		return nil
	})
	conf.Log.Println("waiting handshake")
	err = ins.ctrlChan.Handshake()
	if err != nil {
		return nil, fmt.Errorf("plugin %s control handshake error: %v", pluginMeta.Executable, err)
	}
	ins.process = process
	p.instances[pluginMeta.Name] = ins
	conf.Log.Println("plugin start running")
	// restore symbols by sending commands when restarting plugin
	conf.Log.Info("restore plugin symbols")
	for m, c := range ins.commands {
		go func(key Meta, jsonArg []byte) {
			e := ins.ctrlChan.SendCmd(jsonArg)
			if e != nil {
				conf.Log.Errorf("send command to %v error: %v", key, e)
			}
		}(m, c)
	}

	return ins, nil
}

func (p *pluginInsManager) Kill(name string) error {
	p.Lock()
	defer p.Unlock()
	var err error
	if ins, ok := p.instances[name]; ok {
		err = ins.Stop()
	} else {
		conf.Log.Warnf("instance %s not found when deleting", name)
		return nil
	}
	return err
}

func (p *pluginInsManager) KillAll() error {
	p.Lock()
	defer p.Unlock()
	for _, ins := range p.instances {
		_ = ins.Stop()
	}
	return nil
}

type PluginMeta struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	Language   string `json:"language"`
	Executable string `json:"executable"`
}
