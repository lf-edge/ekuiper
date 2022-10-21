// Copyright 2022 EMQ Technologies Co., Ltd.
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

package meta

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal/conf"
	"strings"
	"sync"
)

type configManager struct {
	lock         sync.RWMutex
	cfgOperators map[string]conf.ConfigOperator
}

//ConfigManager Hold the ConfigOperator for yaml configs defined in etc/sources/xxx.yaml and etc/connections/connection.yaml
// for configs in etc/sources/xxx.yaml, the map key is sources.xxx format, xxx will be mqtt/httppull and so on
// for configs in etc/connections/connection.yaml, the map key is connections.xxx format, xxx will be mqtt/edgex
var ConfigManager = configManager{
	lock:         sync.RWMutex{},
	cfgOperators: make(map[string]conf.ConfigOperator),
}

const SourceCfgOperatorKeyTemplate = "sources.%s"
const SourceCfgOperatorKeyPrefix = "sources."
const SinkCfgOperatorKeyTemplate = "sinks.%s"
const SinkCfgOperatorKeyPrefix = "sinks."
const ConnectionCfgOperatorKeyTemplate = "connections.%s"
const ConnectionCfgOperatorKeyPrefix = "connections."

// loadConfigOperatorForSource
// Try to load ConfigOperator for plugin xxx from /etc/sources/xxx.yaml  /data/sources/xxx.yaml
// If plugin xxx not exist, no error response
func loadConfigOperatorForSource(pluginName string) {
	yamlKey := fmt.Sprintf(SourceCfgOperatorKeyTemplate, pluginName)

	if cfg, _ := conf.NewConfigOperatorFromSourceYaml(pluginName); cfg != nil {
		ConfigManager.lock.Lock()
		ConfigManager.cfgOperators[yamlKey] = cfg
		ConfigManager.lock.Unlock()
		conf.Log.Infof("Loading yaml file for source: %s", pluginName)
	}
}

// loadConfigOperatorForSink
// Try to load ConfigOperator for plugin xxx from /data/sinks/xxx.yaml
// If plugin xxx not exist, no error response
func loadConfigOperatorForSink(pluginName string) {
	yamlKey := fmt.Sprintf(SinkCfgOperatorKeyTemplate, pluginName)

	if cfg, _ := conf.NewConfigOperatorFromSinkYaml(pluginName); cfg != nil {
		ConfigManager.lock.Lock()
		ConfigManager.cfgOperators[yamlKey] = cfg
		ConfigManager.lock.Unlock()
		conf.Log.Infof("Loading yaml file for sink: %s", pluginName)
	}
}

// loadConfigOperatorForConnection
// Try to load ConfigOperator for plugin from /etc/connections/connection.yaml /data/connections/connection.yaml
// If plugin not exist in /etc/connections/connection.yaml, no error response
func loadConfigOperatorForConnection(pluginName string) {
	yamlKey := fmt.Sprintf(ConnectionCfgOperatorKeyTemplate, pluginName)

	if cfg, _ := conf.NewConfigOperatorFromConnectionYaml(pluginName); cfg != nil {
		ConfigManager.lock.Lock()
		ConfigManager.cfgOperators[yamlKey] = cfg
		ConfigManager.lock.Unlock()
		conf.Log.Infof("Loading yaml file for connection: %s", pluginName)
	}
}

func delConfKey(configOperatorKey, confKey, language string) error {
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	cfgOps, ok := ConfigManager.cfgOperators[configOperatorKey]
	if !ok {
		return fmt.Errorf(`%s%s`, getMsg(language, source, "not_found_plugin"), configOperatorKey)
	}

	cfgOps.DeleteConfKey(confKey)

	err := cfgOps.SaveCfgToFile()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, source, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}

func DelSourceConfKey(plgName, confKey, language string) error {
	configOperatorKey := fmt.Sprintf(SourceCfgOperatorKeyTemplate, plgName)
	return delConfKey(configOperatorKey, confKey, language)
}

func DelSinkConfKey(plgName, confKey, language string) error {
	configOperatorKey := fmt.Sprintf(SinkCfgOperatorKeyTemplate, plgName)
	return delConfKey(configOperatorKey, confKey, language)
}

func DelConnectionConfKey(plgName, confKey, language string) error {
	configOperatorKey := fmt.Sprintf(ConnectionCfgOperatorKeyTemplate, plgName)
	return delConfKey(configOperatorKey, confKey, language)
}

func GetYamlConf(configOperatorKey, language string) (b []byte, err error) {

	ConfigManager.lock.RLock()
	defer ConfigManager.lock.RUnlock()

	cfgOps, ok := ConfigManager.cfgOperators[configOperatorKey]
	if !ok {
		return nil, fmt.Errorf(`%s%s`, getMsg(language, source, "not_found_plugin"), configOperatorKey)
	}

	cf := cfgOps.CopyConfContent()
	if b, err = json.Marshal(cf); nil != err {
		return nil, fmt.Errorf(`%s%v`, getMsg(language, source, "json_marshal_fail"), cf)
	} else {
		return b, err
	}
}

func AddSourceConfKey(plgName, confKey, language string, content []byte) error {
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	configOperatorKey := fmt.Sprintf(SourceCfgOperatorKeyTemplate, plgName)

	reqField := make(map[string]interface{})
	err := json.Unmarshal(content, &reqField)
	if nil != err {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, source, "type_conversion_fail"), plgName, err)
	}

	var cfgOps conf.ConfigOperator
	var found bool

	cfgOps, found = ConfigManager.cfgOperators[configOperatorKey]
	if !found {
		cfgOps = conf.NewConfigOperatorForSource(plgName)
		ConfigManager.cfgOperators[configOperatorKey] = cfgOps
	}

	if err := cfgOps.AddConfKey(confKey, reqField); err != nil {
		return err
	}

	err = cfgOps.SaveCfgToFile()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, source, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}

func AddSinkConfKey(plgName, confKey, language string, content []byte) error {
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	configOperatorKey := fmt.Sprintf(SinkCfgOperatorKeyTemplate, plgName)

	reqField := make(map[string]interface{})
	err := json.Unmarshal(content, &reqField)
	if nil != err {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, sink, "type_conversion_fail"), plgName, err)
	}

	var cfgOps conf.ConfigOperator
	var found bool

	cfgOps, found = ConfigManager.cfgOperators[configOperatorKey]
	if !found {
		cfgOps = conf.NewConfigOperatorForSink(plgName)
		ConfigManager.cfgOperators[configOperatorKey] = cfgOps
	}

	if err := cfgOps.AddConfKey(confKey, reqField); err != nil {
		return err
	}

	err = cfgOps.SaveCfgToFile()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, sink, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}

func AddConnectionConfKey(plgName, confKey, language string, content []byte) error {
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	configOperatorKey := fmt.Sprintf(ConnectionCfgOperatorKeyTemplate, plgName)

	reqField := make(map[string]interface{})
	err := json.Unmarshal(content, &reqField)
	if nil != err {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, source, "type_conversion_fail"), plgName, err)
	}

	var cfgOps conf.ConfigOperator
	var found bool

	cfgOps, found = ConfigManager.cfgOperators[configOperatorKey]
	if !found {
		cfgOps = conf.NewConfigOperatorForConnection(plgName)
		ConfigManager.cfgOperators[configOperatorKey] = cfgOps
	}

	if err := cfgOps.AddConfKey(confKey, reqField); err != nil {
		return err
	}

	err = cfgOps.SaveCfgToFile()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, source, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}

func GetResources(language string) (b []byte, err error) {
	ConfigManager.lock.RLock()
	defer ConfigManager.lock.RUnlock()
	var srcResources []map[string]string
	var sinkResources []map[string]string

	for key, ops := range ConfigManager.cfgOperators {
		if strings.HasPrefix(key, ConnectionCfgOperatorKeyPrefix) {
			continue
		}
		if strings.HasPrefix(key, SourceCfgOperatorKeyPrefix) {
			plugin := strings.TrimPrefix(key, SourceCfgOperatorKeyPrefix)
			resourceIds := ops.GetUpdatableConfKeys()
			if len(resourceIds) > 0 {
				item := map[string]string{}
				for _, v := range resourceIds {
					item[v] = plugin
				}
				srcResources = append(srcResources, item)
			}
			continue
		}
		if strings.HasPrefix(key, SinkCfgOperatorKeyPrefix) {
			plugin := strings.TrimPrefix(key, SinkCfgOperatorKeyPrefix)
			resourceIds := ops.GetUpdatableConfKeys()
			if len(resourceIds) > 0 {
				item := map[string]string{}
				for _, v := range resourceIds {
					item[v] = plugin
				}
				sinkResources = append(sinkResources, item)
			}
			continue
		}
	}

	result := map[string]interface{}{}
	result["sources"] = srcResources
	result["sinks"] = sinkResources

	if b, err = json.Marshal(result); nil != err {
		return nil, fmt.Errorf(`%s%v`, getMsg(language, source, "json_marshal_fail"), result)
	} else {
		return b, err
	}
}
