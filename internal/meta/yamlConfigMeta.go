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

package meta

import (
	"encoding/json"
	"fmt"
	"github.com/lf-edge/ekuiper/internal"
	"github.com/lf-edge/ekuiper/internal/conf"
	"sync"
)

type configManager struct {
	lock         sync.RWMutex
	cfgOperators map[string]ConfigOperator
}

//ConfigManager Hold the ConfigOperator for yaml configs defined in etc/sources/xxx.yaml and etc/connections/connection.yaml
// for configs in etc/sources/xxx.yaml, the map key is sources.xxx format, xxx will be mqtt/httppull and so on
// for configs in etc/connections/connection.yaml, the map key is connections.xxx format, xxx will be mqtt/edgex
var ConfigManager = configManager{
	lock:         sync.RWMutex{},
	cfgOperators: make(map[string]ConfigOperator),
}

// loadConfigOperatorForSource
// Try to load ConfigOperator for plugin xxx from /etc/sources/xxx.yaml
// If plugin xxx not exist in /etc/sources/xxx.yaml, no error response
func loadConfigOperatorForSource(pluginName string) {
	yamlKey := fmt.Sprintf(internal.SourceCfgOperatorKeyTemplate, pluginName)

	if cfg, _ := NewConfigOperatorFromSourceYaml(pluginName); cfg != nil {
		ConfigManager.lock.Lock()
		ConfigManager.cfgOperators[yamlKey] = cfg
		ConfigManager.lock.Unlock()
		conf.Log.Infof("Loading yaml file for source: %s", pluginName)
	}
}

// loadConfigOperatorForConnection
// Try to load ConfigOperator for plugin from /etc/connections/connection.yaml
// If plugin not exist in /etc/connections/connection.yaml, no error response
func loadConfigOperatorForConnection(pluginName string) {
	yamlKey := fmt.Sprintf(internal.ConnectionCfgOperatorKeyTemplate, pluginName)

	if cfg, _ := NewConfigOperatorFromConnectionYaml(pluginName); cfg != nil {
		ConfigManager.lock.Lock()
		ConfigManager.cfgOperators[yamlKey] = cfg
		ConfigManager.lock.Unlock()
		conf.Log.Infof("Loading yaml file for connection: %s", pluginName)
	}
}

func GetYamlConf(configOperatorKey, language string) (b []byte, err error) {

	ConfigManager.lock.RLock()
	defer ConfigManager.lock.RUnlock()

	cfgOps, ok := ConfigManager.cfgOperators[configOperatorKey]
	if !ok {
		return nil, fmt.Errorf(`%s%s`, getMsg(language, internal.Source, "not_found_plugin"), configOperatorKey)
	}

	cf := cfgOps.CopyConfContent()
	if b, err = json.Marshal(cf); nil != err {
		return nil, fmt.Errorf(`%s%v`, getMsg(language, internal.Source, "json_marshal_fail"), cf)
	} else {
		return b, err
	}
}

func DelSourceConfKey(plgName, confKey, language string) error {
	configOperatorKey := fmt.Sprintf(internal.SourceCfgOperatorKeyTemplate, plgName)
	return delConfKey(configOperatorKey, confKey, language)
}

func DelConnectionConfKey(plgName, confKey, language string) error {
	configOperatorKey := fmt.Sprintf(internal.ConnectionCfgOperatorKeyTemplate, plgName)
	return delConfKey(configOperatorKey, confKey, language)
}

func delConfKey(configOperatorKey, confKey, language string) error {
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	cfgOps, ok := ConfigManager.cfgOperators[configOperatorKey]
	if !ok {
		return fmt.Errorf(`%s%s`, getMsg(language, internal.Source, "not_found_plugin"), configOperatorKey)
	}

	cfgOps.DeleteConfKey(confKey)

	err := cfgOps.SaveCfgToFile()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, internal.Source, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}

func AddSourceConfKey(plgName, confKey, language string, content []byte) error {
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	configOperatorKey := fmt.Sprintf(internal.SourceCfgOperatorKeyTemplate, plgName)

	reqField := make(map[string]interface{})
	err := json.Unmarshal(content, &reqField)
	if nil != err {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, internal.Source, "type_conversion_fail"), plgName, err)
	}

	var cfgOps ConfigOperator
	var found bool

	cfgOps, found = ConfigManager.cfgOperators[configOperatorKey]
	if !found {
		cfgOps = &SourceConfigKeysOps{
			ConfigKeys: &ConfigKeys{
				lock:       sync.RWMutex{},
				pluginName: plgName,
				cf:         map[string]map[string]interface{}{},
			},
		}
		ConfigManager.cfgOperators[configOperatorKey] = cfgOps
	}

	if err := cfgOps.AddConfKey(confKey, reqField); err != nil {
		return err
	}

	err = cfgOps.SaveCfgToFile()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, internal.Source, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}

func AddConnectionConfKey(plgName, confKey, language string, content []byte) error {
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	configOperatorKey := fmt.Sprintf(internal.ConnectionCfgOperatorKeyTemplate, plgName)

	reqField := make(map[string]interface{})
	err := json.Unmarshal(content, &reqField)
	if nil != err {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, internal.Source, "type_conversion_fail"), plgName, err)
	}

	var cfgOps ConfigOperator
	var found bool

	cfgOps, found = ConfigManager.cfgOperators[configOperatorKey]
	if !found {
		cfgOps = &ConnectionConfigKeysOps{
			ConfigKeys: &ConfigKeys{
				lock:       sync.RWMutex{},
				pluginName: plgName,
				cf:         map[string]map[string]interface{}{},
			},
		}
		ConfigManager.cfgOperators[configOperatorKey] = cfgOps
	}

	if err := cfgOps.AddConfKey(confKey, reqField); err != nil {
		return err
	}

	err = cfgOps.SaveCfgToFile()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, internal.Source, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}
