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

// Plugin runtime to control the whole plugin with control channel: Distribute symbol data connection, stop symbol and stop plugin

package runtime

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/sdk/api"
	"github.com/lf-edge/ekuiper/sdk/connection"
	"github.com/lf-edge/ekuiper/sdk/context"
	"go.nanomsg.org/mangos/v3"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	logger api.Logger
	reg    runtimes
)

func initVars(args []string, conf *PluginConfig) {
	logger = context.LogEntry("plugin", conf.Name)
	reg = runtimes{
		content: make(map[string]RuntimeInstance),
		RWMutex: sync.RWMutex{},
	}
	// parse Args
	if len(args) == 2 {
		pc := &PortableConfig{}
		err := json.Unmarshal([]byte(args[1]), pc)
		if err != nil {
			panic(fmt.Sprintf("fail to parse args %v", args))
		}
		connection.Options = map[string]interface{}{
			mangos.OptionSendDeadline: pc.SendTimeout,
		}
		logger.Infof("config parsed to %v", pc)
	} else {
		connection.Options = make(map[string]interface{})
	}
}

type NewSourceFunc func() api.Source
type NewFunctionFunc func() api.Function
type NewSinkFunc func() api.Sink

// PluginConfig construct once and then read only
type PluginConfig struct {
	Name      string
	Sources   map[string]NewSourceFunc
	Functions map[string]NewFunctionFunc
	Sinks     map[string]NewSinkFunc
}

func (conf *PluginConfig) Get(pluginType string, symbolName string) (builderFunc interface{}) {
	switch pluginType {
	case TYPE_SOURCE:
		if f, ok := conf.Sources[symbolName]; ok {
			return f
		}
	case TYPE_FUNC:
		if f, ok := conf.Functions[symbolName]; ok {
			return f
		}
	case TYPE_SINK:
		if f, ok := conf.Sinks[symbolName]; ok {
			return f
		}
	}
	return nil
}

// Start Connect to control plane
// Only run once at process startup
func Start(args []string, conf *PluginConfig) {
	initVars(args, conf)
	logger.Info("starting plugin")
	ch, err := connection.CreateControlChannel(conf.Name)
	if err != nil {
		panic(err)
	}
	defer ch.Close()
	go func() {
		logger.Info("running control channel")
		err = ch.Run(func(req []byte) []byte { // not parallel run now
			c := &Command{}
			err := json.Unmarshal(req, c)
			if err != nil {
				return []byte(err.Error())
			}
			logger.Infof("received command %s with arg:'%s'", c.Cmd, c.Arg)
			ctrl := &Control{}
			err = json.Unmarshal([]byte(c.Arg), ctrl)
			if err != nil {
				return []byte(err.Error())
			}
			switch c.Cmd {
			case CMD_START:
				f := conf.Get(ctrl.PluginType, ctrl.SymbolName)
				if f == nil {
					return []byte("symbol not found")
				}
				switch ctrl.PluginType {
				case TYPE_SOURCE:
					sf := f.(NewSourceFunc)
					sr, err := setupSourceRuntime(ctrl, sf())
					if err != nil {
						return []byte(err.Error())
					}
					go sr.run()
					regKey := fmt.Sprintf("%s_%s_%d_%s", ctrl.Meta.RuleId, ctrl.Meta.OpId, ctrl.Meta.InstanceId, ctrl.SymbolName)
					reg.Set(regKey, sr)
					logger.Infof("running source %s", ctrl.SymbolName)
				case TYPE_SINK:
					sf := f.(NewSinkFunc)
					sr, err := setupSinkRuntime(ctrl, sf())
					if err != nil {
						return []byte(err.Error())
					}
					go sr.run()
					regKey := fmt.Sprintf("%s_%s_%d_%s", ctrl.Meta.RuleId, ctrl.Meta.OpId, ctrl.Meta.InstanceId, ctrl.SymbolName)
					reg.Set(regKey, sr)
					logger.Infof("running sink %s", ctrl.SymbolName)
				case TYPE_FUNC:
					regKey := fmt.Sprintf("func_%s", ctrl.SymbolName)
					_, ok := reg.Get(regKey)
					if ok {
						logger.Infof("got running function instance %s, do nothing", ctrl.SymbolName)
					} else {
						ff := f.(NewFunctionFunc)
						fr, err := setupFuncRuntime(ctrl, ff())
						if err != nil {
							return []byte(err.Error())
						}
						go fr.run()
						reg.Set(regKey, fr)
						logger.Infof("running function %s", ctrl.SymbolName)
					}
				default:
					return []byte(fmt.Sprintf("invalid plugin type %s", ctrl.PluginType))
				}
				return []byte(REPLY_OK)
			case CMD_STOP:
				// never stop a function symbol here.
				regKey := fmt.Sprintf("%s_%s_%d_%s", ctrl.Meta.RuleId, ctrl.Meta.OpId, ctrl.Meta.InstanceId, ctrl.SymbolName)
				logger.Infof("stopping %s", regKey)
				runtime, ok := reg.Get(regKey)
				if !ok {
					return []byte(fmt.Sprintf("symbol %s not found", regKey))
				}
				if runtime.isRunning() {
					err = runtime.stop()
					if err != nil {
						return []byte(err.Error())
					}
				}
				return []byte(REPLY_OK)
			default:
				return []byte(fmt.Sprintf("invalid command received: %s", c.Cmd))
			}
		})
		if err != nil {
			logger.Error(err)
		}
		os.Exit(1)
	}()
	//Stop the whole plugin
	sigint := make(chan os.Signal, 1)
	signal.Notify(sigint, os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	<-sigint
	logger.Infof("stopping plugin %s", conf.Name)
	os.Exit(0)
}

// key is rule_op_ins_symbol
type runtimes struct {
	content map[string]RuntimeInstance
	sync.RWMutex
}

func (r *runtimes) Set(name string, instance RuntimeInstance) {
	r.Lock()
	defer r.Unlock()
	r.content[name] = instance
}

func (r *runtimes) Get(name string) (RuntimeInstance, bool) {
	r.RLock()
	defer r.RUnlock()
	result, ok := r.content[name]
	return result, ok
}

func (r *runtimes) Delete(name string) {
	r.Lock()
	defer r.Unlock()
	delete(r.content, name)
}
