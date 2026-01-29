// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/pingcap/failpoint"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/infra"
	"github.com/lf-edge/ekuiper/v2/pkg/syncx"
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
// 2. At system start/restart
// Once created, never deleted until system shutdown
type PluginIns struct {
	syncx.RWMutex
	name     string
	ctrlChan ControlChannel // the same lifecycle as pluginIns, once created keep listening
	// audit the commands, so that when restarting the plugin, we can replay the commands
	commands map[Meta][]byte
	process  *os.Process // created when used by rule and deleted when delete the plugin
	Status   *PluginStatus
}

func NewPluginIns(name string, ctrlChan ControlChannel, process *os.Process) *PluginIns {
	return &PluginIns{
		process:  process,
		ctrlChan: ctrlChan,
		name:     name,
		commands: make(map[Meta][]byte),
		Status:   NewPluginStatus(),
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
		process:  &os.Process{},
		ctrlChan: ctrlChan,
		name:     name,
		commands: commands,
		Status:   NewPluginStatus(),
	}
}

func (i *PluginIns) sendCmd(jsonArg []byte) error {
	err := i.ctrlChan.SendCmd(jsonArg)
	if err != nil && i.process == nil {
		return fmt.Errorf("plugin %s is not running successfully, please make sure it is valid", i.name)
	}
	return err
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
	err = i.sendCmd(jsonArg)
	if err == nil {
		i.Lock()
		i.addRef(ctx)
		i.commands[ctrl.Meta] = jsonArg
		i.Unlock()
		ctx.GetLogger().Infof("started symbol %s", ctrl.SymbolName)
	}
	return err
}

func (i *PluginIns) addRef(ctx api.StreamContext) {
	ruleID := ctx.GetRuleId()
	if len(ruleID) < 1 {
		return
	}
	cnt, ok := i.Status.RefCount[ruleID]
	if ok {
		i.Status.RefCount[ruleID] = cnt + 1
	} else {
		i.Status.RefCount[ruleID] = 1
	}
}

func (i *PluginIns) deRef(ctx api.StreamContext) {
	ruleID := ctx.GetRuleId()
	if len(ruleID) < 1 {
		return
	}
	cnt, ok := i.Status.RefCount[ruleID]
	if ok {
		if cnt > 1 {
			i.Status.RefCount[ruleID] = cnt - 1
			return
		}
		delete(i.Status.RefCount, ruleID)
	}
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
	err = i.sendCmd(jsonArg)
	if err == nil {
		i.Lock()
		delete(i.commands, ctrl.Meta)
		i.deRef(ctx)
		i.Unlock()
		ctx.GetLogger().Infof("stopped symbol %s", ctrl.SymbolName)
	}
	return err
}

// Stop intentionally
func (i *PluginIns) Stop() error {
	var err error
	i.RLock()
	defer i.RUnlock()
	i.Status.Stop()
	if i.process != nil {
		err = i.process.Kill()
		i.process = nil
	}
	return err
}

func (i *PluginIns) GetStatus() *PluginStatus {
	i.RLock()
	defer i.RUnlock()
	return i.Status
}

// Manager plugin process and control socket
type pluginInsManager struct {
	instances map[string]*PluginIns
	syncx.RWMutex
}

func GetPluginInsManager() *pluginInsManager {
	once.Do(func() {
		pm = &pluginInsManager{
			instances: make(map[string]*PluginIns),
		}
	})
	return pm
}

func (p *pluginInsManager) GetPluginInsStatus(name string) (*PluginStatus, bool) {
	ins, ok := p.getPluginIns(name)
	if !ok {
		return nil, false
	}
	return ins.GetStatus(), true
}

func (p *pluginInsManager) getPluginIns(name string) (*PluginIns, bool) {
	p.RLock()
	defer p.RUnlock()
	ins, ok := p.instances[name]
	return ins, ok
}

// AddPluginIns For mock only
func (p *pluginInsManager) AddPluginIns(name string, ins *PluginIns) {
	p.Lock()
	defer p.Unlock()
	p.instances[name] = ins
}

// CreateIns Run when plugin is created/updated
func (p *pluginInsManager) CreateIns(pluginMeta *PluginMeta) (*PluginIns, error) {
	return p.GetOrStartProcess(pluginMeta, PortbleConf)
}

// GetOrStartProcess Control the plugin process lifecycle.
// Need to manage the resources: instances map, control socket, plugin process
// May be called at plugin creation or restart with previous state(ctrlCh, commands)
// PluginIns is created by plugin manager and started immediately or restart by rule/funcop.
// The ins is long running. Even for plugin delete/update, the ins will continue. So there is no delete.
// 1. During creation, clean up those resources for any errors in defer immediately after the resource is created.
// 2. During plugin running, when detecting plugin process exit, clean up those resources for the current ins.
func (p *pluginInsManager) GetOrStartProcess(pluginMeta *PluginMeta, pconf *PortableConfig) (_ *PluginIns, e error) {
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
	// ins has run
	if ins.process != nil && ins.ctrlChan != nil {
		return ins, nil
	}
	// should only happen for first start, then the ctrl channel will keep running
	if ins.ctrlChan == nil {
		conf.Log.Infof("create control channel")
		ctrlChan, err := CreateControlChannel(pluginMeta.Name)
		if err != nil {
			ins.Status.StatusErr(err)
			return nil, fmt.Errorf("can't create new control channel: %s", err.Error())
		}
		ins.ctrlChan = ctrlChan
	}
	// init or restart all need to run the process
	conf.Log.Infof("executing plugin")
	jsonArg, err := json.Marshal(pconf)
	failpoint.Inject("confErr", func() {
		err = errors.New("confErr")
	})
	if err != nil {
		ins.Status.StatusErr(err)
		return nil, fmt.Errorf("invalid conf: %v", pconf)
	}
	var cmd *exec.Cmd
	err = infra.SafeRun(func() error {
		switch pluginMeta.Language {
		case "go":
			conf.Log.Printf("starting go plugin executable %s", pluginMeta.Executable)
			cmd = exec.Command(pluginMeta.Executable, string(jsonArg))
		case "python":
			if pluginMeta.VirtualType != nil {
				switch *pluginMeta.VirtualType {
				case "conda":
					cmd = exec.Command("conda", "run", "-n", *pluginMeta.Env, conf.Config.Portable.PythonBin, pluginMeta.Executable, string(jsonArg))
				default:
					err = fmt.Errorf("unsupported virtual type: %s", *pluginMeta.VirtualType)
					return err
				}
			}
			if cmd == nil {
				cmd = exec.Command(conf.Config.Portable.PythonBin, "-u", pluginMeta.Executable, string(jsonArg))
			}
			conf.Log.Infof("starting python plugin: %s", cmd)
		default:
			err := fmt.Errorf("unsupported language: %s", pluginMeta.Language)
			return err
		}
		return nil
	})
	if err != nil {
		ins.Status.StatusErr(err)
		return nil, fmt.Errorf("fail to start plugin %s: %v", pluginMeta.Name, err)
	}
	cmd.Stdout = conf.Log.Out
	cmd.Stderr = conf.Log.Out
	cmd.Dir = filepath.Dir(pluginMeta.Executable)
	conf.Log.Println("plugin starting")
	err = cmd.Start()
	failpoint.Inject("cmdStartErr", func() {
		cmd.Process.Kill()
		err = errors.New("cmdStartErr")
	})
	if err != nil {
		ins.Status.StatusErr(err)
		return nil, fmt.Errorf("plugin executable %s stops with error %v", pluginMeta.Executable, err)
	}
	process := cmd.Process
	conf.Log.Printf("plugin started pid: %d\n", process.Pid)
	defer func() {
		if e != nil {
			_ = process.Kill()
		}
	}()
	go infra.SafeRun(func() error { // just print out error inside
		err = cmd.Wait()
		if err != nil {
			ins.Status.StatusErr(err)
			conf.Log.Printf("plugin executable %s stops with error %v", pluginMeta.Executable, err)
		}
		// must make sure the plugin ins is not cleaned up yet by checking the process identity
		// clean up for stop unintentionally
		if ins, ok := p.getPluginIns(pluginMeta.Name); ok && ins.process == cmd.Process {
			ins.Lock()
			ins.process = nil
			ins.Unlock()
		}
		return nil
	})
	conf.Log.Println("waiting handshake")
	err = ins.ctrlChan.Handshake()
	if err != nil {
		ins.Status.StatusErr(err)
		return nil, fmt.Errorf("plugin %s control handshake error: %v", pluginMeta.Executable, err)
	}
	ins.process = process
	p.instances[pluginMeta.Name] = ins
	conf.Log.Println("plugin start running")
	ins.Status.StartRunning()
	// restore symbols by sending commands when restarting plugin
	conf.Log.Info("restore plugin symbols")
	for m, c := range ins.commands {
		go func(key Meta, jsonArg []byte) {
			e := ins.sendCmd(jsonArg)
			if e != nil {
				ins.Status.StatusErr(e)
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
	Name        string  `json:"name"`
	Version     string  `json:"version"`
	Language    string  `json:"language"`
	Executable  string  `json:"executable"`
	VirtualType *string `json:"virtualEnvType,omitempty"`
	Env         *string `json:"env,omitempty"`
}

const (
	PluginStatusRunning = "running"
	PluginStatusInit    = "initializing"
	PluginStatusErr     = "error"
	PluginStatusStop    = "stop"
)

type PluginStatus struct {
	RefCount map[string]int `json:"refCount"`
	Status   string         `json:"status"`
	ErrMsg   string         `json:"errMsg"`
}

func NewPluginStatus() *PluginStatus {
	return &PluginStatus{
		RefCount: make(map[string]int),
		Status:   PluginStatusInit,
	}
}

func (s *PluginStatus) StatusErr(err error) {
	s.Status = PluginStatusErr
	s.ErrMsg = err.Error()
}

func (s *PluginStatus) StartRunning() {
	s.Status = PluginStatusRunning
	s.ErrMsg = ""
}

func (s *PluginStatus) Stop() {
	s.Status = PluginStatusStop
	s.ErrMsg = ""
}

func (s *PluginStatus) GetRuleRefCount(rule string) int {
	cnt, ok := s.RefCount[rule]
	if !ok {
		return 0
	}
	return cnt
}
