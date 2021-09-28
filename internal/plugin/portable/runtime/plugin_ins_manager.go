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

package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/api"
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

type PluginIns struct {
	process      *os.Process
	ctrlChan     ControlChannel
	runningCount int
	name         string
}

func NewPluginIns(name string, ctrlChan ControlChannel, process *os.Process) *PluginIns {
	return &PluginIns{
		process:      process,
		ctrlChan:     ctrlChan,
		runningCount: 0,
		name:         name,
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
		i.runningCount++
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
	if i.process != nil {
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

func (p *pluginInsManager) deletePluginIns(name string) {
	p.Lock()
	defer p.Unlock()
	delete(p.instances, name)
}

func (p *pluginInsManager) getOrStartProcess(pluginMeta *PluginMeta, pconf *PortableConfig) (*PluginIns, error) {
	p.Lock()
	defer p.Unlock()
	if ins, ok := p.instances[pluginMeta.Name]; ok {
		return ins, nil
	}

	conf.Log.Infof("create control channel")
	ctrlChan, err := CreateControlChannel(pluginMeta.Name)
	if err != nil {
		return nil, fmt.Errorf("can't create new control channel: %s", err.Error())
	}

	conf.Log.Infof("executing plugin")
	jsonArg, err := json.Marshal(pconf)
	if err != nil {
		return nil, fmt.Errorf("invalid conf: %v", pconf)
	}
	var cmd *exec.Cmd
	switch pluginMeta.Language {
	case "go":
		conf.Log.Printf("starting go plugin executable %s", pluginMeta.Executable)
		cmd = exec.Command(pluginMeta.Executable, string(jsonArg))

	case "python":
		conf.Log.Printf("starting python plugin executable %s\n", pluginMeta.Executable)
		cmd = exec.Command("python", pluginMeta.Executable, string(jsonArg))
	default:
		return nil, fmt.Errorf("unsupported language: %s", pluginMeta.Language)
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
	go func() {
		err = cmd.Wait()
		if err != nil {
			conf.Log.Printf("plugin executable %s stops with error %v", pluginMeta.Executable, err)
		}

		if ins, ok := p.getPluginIns(pluginMeta.Name); ok {
			_ = ins.ctrlChan.Close()
			p.deletePluginIns(pluginMeta.Name)
		}
	}()

	conf.Log.Println("waiting handshake")
	err = ctrlChan.Handshake()
	if err != nil {
		return nil, fmt.Errorf("plugin %s control handshake error: %v", pluginMeta.Executable, err)
	}

	ins := NewPluginIns(pluginMeta.Name, ctrlChan, process)
	p.instances[pluginMeta.Name] = ins
	conf.Log.Println("plugin start running")
	return ins, nil
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

func (p *pluginInsManager) KillAll() error {
	p.Lock()
	defer p.Unlock()
	for _, ins := range p.instances {
		_ = ins.Stop()
	}
	p.instances = make(map[string]*PluginIns)
	return nil
}

type PluginMeta struct {
	Name       string `json:"name"`
	Version    string `json:"version"`
	Language   string `json:"language"`
	Executable string `json:"executable"`
}
