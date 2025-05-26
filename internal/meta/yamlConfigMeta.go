// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"strings"
	"sync"

	"github.com/lf-edge/ekuiper/v2/internal/binder/io"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/store"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/util"
	"github.com/lf-edge/ekuiper/v2/pkg/cast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
	"github.com/lf-edge/ekuiper/v2/pkg/errorx"
	"github.com/lf-edge/ekuiper/v2/pkg/kv"
	"github.com/lf-edge/ekuiper/v2/pkg/replace"
)

type configManager struct {
	lock                     sync.RWMutex
	cfgOperators             map[string]conf.ConfigOperator
	sourceConfigStatusDb     kv.KeyValue
	sinkConfigStatusDb       kv.KeyValue
	connectionConfigStatusDb kv.KeyValue
}

// ConfigManager Hold the ConfigOperator for yaml configs defined in etc/sources/xxx.yaml and etc/connections/connection.yaml
// for configs in etc/sources/xxx.yaml, the map key is sources.xxx format, xxx will be mqtt/httppull and so on
// for configs in etc/connections/connection.yaml, the map key is connections.xxx format, xxx will be mqtt/edgex
var ConfigManager *configManager

func InitYamlConfigManager() {
	ConfigManager = &configManager{
		lock:         sync.RWMutex{},
		cfgOperators: make(map[string]conf.ConfigOperator),
	}
	ConfigManager.sourceConfigStatusDb, _ = store.GetKV("sourceConfigStatus")
	ConfigManager.sinkConfigStatusDb, _ = store.GetKV("sinkConfigStatus")
	ConfigManager.connectionConfigStatusDb, _ = store.GetKV("connectionConfigStatus")
}

const (
	SourceCfgOperatorKeyTemplate     = "sources.%s"
	SourceCfgOperatorKeyPrefix       = "sources."
	SinkCfgOperatorKeyTemplate       = "sinks.%s"
	SinkCfgOperatorKeyPrefix         = "sinks."
	ConnectionCfgOperatorKeyTemplate = "connections.%s"
	ConnectionCfgOperatorKeyPrefix   = "connections."
)

// loadConfigOperatorForSource
// Try to load ConfigOperator for plugin xxx from /etc/sources/xxx.yaml  /data/sources/xxx.yaml
// If plugin xxx not exist, no error response
func loadConfigOperatorForSource(pluginName string) {
	yamlKey := fmt.Sprintf(SourceCfgOperatorKeyTemplate, pluginName)

	if cfg, _ := conf.NewConfigOperatorFromSourceStorage(pluginName); cfg != nil {
		ConfigManager.lock.Lock()
		ConfigManager.cfgOperators[yamlKey] = cfg
		ConfigManager.lock.Unlock()
	}
}

// loadConfigOperatorForSink
// Try to load ConfigOperator for plugin xxx from /data/sinks/xxx.yaml
// If plugin xxx not exist, no error response
func loadConfigOperatorForSink(pluginName string) {
	yamlKey := fmt.Sprintf(SinkCfgOperatorKeyTemplate, pluginName)

	if cfg, _ := conf.NewConfigOperatorFromSinkStorage(pluginName); cfg != nil {
		ConfigManager.lock.Lock()
		ConfigManager.cfgOperators[yamlKey] = cfg
		ConfigManager.lock.Unlock()
	}
}

// loadConfigOperatorForConnection
// Try to load ConfigOperator for plugin from /etc/connections/connection.yaml /data/connections/connection.yaml
// If plugin not exist in /etc/connections/connection.yaml, no error response
func loadConfigOperatorForConnection(pluginName string) {
	yamlKey := fmt.Sprintf(ConnectionCfgOperatorKeyTemplate, pluginName)

	if cfg, _ := conf.NewConfigOperatorFromConnectionStorage(pluginName); cfg != nil {
		ConfigManager.lock.Lock()
		ConfigManager.cfgOperators[yamlKey] = cfg
		conns := cfg.CopyConfContent()
		for id, props := range conns {
			err := conf.WriteCfgIntoKVStorage("connections", pluginName, id, props)
			if err != nil {
				conf.Log.Errorf("save connection %s err:%v", yamlKey, err)
			}
		}
		ConfigManager.lock.Unlock()
	}
}

func delConfKey(configOperatorKey, confKey, language string) (err error) {
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	cfgOps, ok := ConfigManager.cfgOperators[configOperatorKey]
	if !ok {
		return fmt.Errorf(`%s%s`, getMsg(language, source, "not_found_plugin"), configOperatorKey)
	}

	cfgOps.DeleteConfKey(confKey)

	err = cfgOps.SaveCfgToStorage()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, source, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}

func DelSourceConfKey(plgName, confKey, language string) (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	configOperatorKey := fmt.Sprintf(SourceCfgOperatorKeyTemplate, plgName)
	return delConfKey(configOperatorKey, confKey, language)
}

func DelSinkConfKey(plgName, confKey, language string) (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	configOperatorKey := fmt.Sprintf(SinkCfgOperatorKeyTemplate, plgName)
	return delConfKey(configOperatorKey, confKey, language)
}

func DelConnectionConfKey(plgName, confKey, language string) (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	configOperatorKey := fmt.Sprintf(ConnectionCfgOperatorKeyTemplate, plgName)
	return delConfKey(configOperatorKey, confKey, language)
}

func delYamlConf(configOperatorKey string) {
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	_, ok := ConfigManager.cfgOperators[configOperatorKey]
	if ok {
		delete(ConfigManager.cfgOperators, configOperatorKey)
	}
}

func GetConfOperator(configOperatorKey string) (conf.ConfigOperator, bool) {
	ConfigManager.lock.RLock()
	defer ConfigManager.lock.RUnlock()
	cfgOps, ok := ConfigManager.cfgOperators[configOperatorKey]
	return cfgOps, ok
}

func GetSourceResourceConf(sourceType string) map[string]map[string]map[string]interface{} {
	ConfigManager.lock.RLock()
	defer ConfigManager.lock.RUnlock()
	result := make(map[string]map[string]map[string]interface{})
	if sourceType == "" {
		for k, v := range ConfigManager.cfgOperators {
			if strings.HasPrefix(k, "sources.") {
				typ := k[len("sources."):]
				for name, c := range v.CopyConfContent() {
					appendConfKeyInResult(result, typ, name, c)
				}
			}
		}
		return result
	}
	// specific sourceType
	for k, v := range ConfigManager.cfgOperators {
		if strings.HasPrefix(k, "sources.") {
			typ := k[len("sources."):]
			for name, c := range v.CopyConfContent() {
				value, ok := c["sourceType"]
				if ok {
					sv, ok := value.(string)
					if ok && sv == sourceType {
						appendConfKeyInResult(result, typ, name, c)
					}
				}
			}
		}
	}
	return result
}

func appendConfKeyInResult(result map[string]map[string]map[string]interface{}, typ, confKey string, confValue map[string]interface{}) {
	v1, ok := result[typ]
	if !ok {
		result[typ] = make(map[string]map[string]interface{})
		v1 = result[typ]
	}
	v1[confKey] = confValue
}

func GetYamlConf(configOperatorKey, language string) (b []byte, err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	ConfigManager.lock.RLock()
	defer ConfigManager.lock.RUnlock()

	cfgOps, ok := ConfigManager.cfgOperators[configOperatorKey]
	if !ok {
		return nil, fmt.Errorf(`%s%s`, getMsg(language, source, "not_found_plugin"), configOperatorKey)
	}

	cf := cfgOps.CopyConfContent()
	for plug, props := range cf {
		changed, newProps := replace.ReplacePropsWithPlug(plug, props)
		if changed {
			cf[plug] = newProps
		}
	}

	if b, err = json.Marshal(cf); nil != err {
		return nil, fmt.Errorf(`%s%v`, getMsg(language, source, "json_marshal_fail"), cf)
	} else {
		return b, err
	}
}

func replaceConfigurations(plg string, cf YamlConfigurations) YamlConfigurations {
	for key, props := range cf {
		replaced, newProps := replace.ReplacePropsWithPlug(plg, props)
		if replaced {
			cf[key] = newProps
		}
	}
	return cf
}

func addSourceConfKeys(plgName string, configurations YamlConfigurations) (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	configurations = replaceConfigurations(plgName, configurations)

	configOperatorKey := fmt.Sprintf(SourceCfgOperatorKeyTemplate, plgName)

	var cfgOps conf.ConfigOperator
	var found bool

	cfgOps, found = ConfigManager.cfgOperators[configOperatorKey]
	if !found {
		cfgOps = conf.NewConfigOperatorForSource(plgName)
		ConfigManager.cfgOperators[configOperatorKey] = cfgOps
	}

	cfgOps.LoadConfContent(configurations)

	err = cfgOps.SaveCfgToStorage()
	if err != nil {
		return fmt.Errorf(`%s.%v`, configOperatorKey, err)
	}
	return nil
}

func AddSourceConfKey(plgName, confKey, language string, content []byte) (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	configOperatorKey := fmt.Sprintf(SourceCfgOperatorKeyTemplate, plgName)

	reqField := make(map[string]interface{})
	err = json.Unmarshal(content, &reqField)
	if nil != err {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, source, "type_conversion_fail"), plgName, err)
	}
	var cfgOps conf.ConfigOperator
	var found bool

	if err := validateConf(plgName, reqField, true); err != nil {
		return err
	}

	_, reqField = replace.ReplacePropsWithPlug(plgName, reqField)
	cfgOps, found = ConfigManager.cfgOperators[configOperatorKey]
	if !found {
		cfgOps = conf.NewConfigOperatorForSource(plgName)
		ConfigManager.cfgOperators[configOperatorKey] = cfgOps
	}

	if err := cfgOps.AddConfKey(confKey, reqField); err != nil {
		return err
	}

	err = cfgOps.SaveCfgToStorage()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, source, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}

func validateConf(pluginName string, props map[string]interface{}, isSource bool) error {
	m := io.GetManager()
	if !isSource {
		s, err := m.Sink(pluginName)
		if err != nil {
			return err
		}
		if v, ok := s.(util.PropsValidator); ok {
			return v.Validate(props)
		}
		return nil
	} else {
		s, err := m.Source(pluginName)
		if err != nil {
			return err
		}
		if v, ok := s.(util.PropsValidator); ok {
			return v.Validate(props)
		}
		return nil
	}
}

func AddSinkConfKey(plgName, confKey, language string, content []byte) (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	configOperatorKey := fmt.Sprintf(SinkCfgOperatorKeyTemplate, plgName)

	reqField := make(map[string]interface{})
	err = json.Unmarshal(content, &reqField)
	if nil != err {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, sink, "type_conversion_fail"), plgName, err)
	}
	if err := validateConf(plgName, reqField, false); err != nil {
		return err
	}
	_, reqField = replace.ReplacePropsWithPlug(plgName, reqField)
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

	err = cfgOps.SaveCfgToStorage()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, sink, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}

func addSinkConfKeys(plgName string, cf YamlConfigurations) (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	cf = replaceConfigurations(plgName, cf)

	configOperatorKey := fmt.Sprintf(SinkCfgOperatorKeyTemplate, plgName)

	var cfgOps conf.ConfigOperator
	var found bool

	cfgOps, found = ConfigManager.cfgOperators[configOperatorKey]
	if !found {
		cfgOps = conf.NewConfigOperatorForSink(plgName)
		ConfigManager.cfgOperators[configOperatorKey] = cfgOps
	}

	cfgOps.LoadConfContent(cf)

	err = cfgOps.SaveCfgToStorage()
	if err != nil {
		return fmt.Errorf(`%s.%v`, configOperatorKey, err)
	}
	return nil
}

func AddConnectionConfKey(plgName, confKey, language string, reqField map[string]interface{}) (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	configOperatorKey := fmt.Sprintf(ConnectionCfgOperatorKeyTemplate, plgName)
	_, reqField = replace.ReplacePropsWithPlug(plgName, reqField)
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

	err = cfgOps.SaveCfgToStorage()
	if err != nil {
		return fmt.Errorf(`%s%s.%v`, getMsg(language, source, "write_data_fail"), configOperatorKey, err)
	}
	return nil
}

func addConnectionConfKeys(plgName string, cf YamlConfigurations) (err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	cf = replaceConfigurations(plgName, cf)

	configOperatorKey := fmt.Sprintf(ConnectionCfgOperatorKeyTemplate, plgName)

	var cfgOps conf.ConfigOperator
	var found bool

	cfgOps, found = ConfigManager.cfgOperators[configOperatorKey]
	if !found {
		cfgOps = conf.NewConfigOperatorForConnection(plgName)
		ConfigManager.cfgOperators[configOperatorKey] = cfgOps
	}

	cfgOps.LoadConfContent(cf)

	err = cfgOps.SaveCfgToStorage()
	if err != nil {
		return fmt.Errorf(`%s.%v`, configOperatorKey, err)
	}
	return nil
}

func GetResources(language string) (b []byte, err error) {
	defer func() {
		if err != nil {
			if _, ok := err.(errorx.ErrorWithCode); !ok {
				err = errorx.NewWithCode(errorx.ConfKeyError, err.Error())
			}
		}
	}()
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

func ResetConfigs() {
	ConfigManager.lock.Lock()
	defer ConfigManager.lock.Unlock()

	for _, ops := range ConfigManager.cfgOperators {
		ops.ClearConfKeys()
		_ = ops.SaveCfgToStorage()
	}
}

type YamlConfigurations map[string]map[string]interface{}

type YamlConfigurationSet struct {
	Sources     map[string]string `json:"sources"`
	Sinks       map[string]string `json:"sinks"`
	Connections map[string]string `json:"connections"`
}

func GetConfigurations() YamlConfigurationSet {
	ConfigManager.lock.RLock()
	defer ConfigManager.lock.RUnlock()
	result := YamlConfigurationSet{
		Sources:     map[string]string{},
		Sinks:       map[string]string{},
		Connections: map[string]string{},
	}
	srcResources := map[string]string{}
	sinkResources := map[string]string{}
	connectionResources := map[string]string{}

	for key, ops := range ConfigManager.cfgOperators {
		if strings.HasPrefix(key, SourceCfgOperatorKeyPrefix) {
			plugin := strings.TrimPrefix(key, SourceCfgOperatorKeyPrefix)
			cfs := ops.CopyUpdatableConfContent()
			if len(cfs) > 0 {
				jsonByte, _ := json.Marshal(cfs)
				srcResources[plugin] = string(jsonByte)
			}
			continue
		}
		if strings.HasPrefix(key, SinkCfgOperatorKeyPrefix) {
			plugin := strings.TrimPrefix(key, SinkCfgOperatorKeyPrefix)
			cfs := ops.CopyUpdatableConfContent()
			if len(cfs) > 0 {
				jsonByte, _ := json.Marshal(cfs)
				sinkResources[plugin] = string(jsonByte)
			}
			continue
		}
	}
	connConfigs, err := conf.GetAllConnConfigs()
	if err != nil {
		conf.Log.Warnf("export connections err:%v", err)
	} else {
		for plugin, props := range connConfigs {
			jsonByte, _ := json.Marshal(props)
			connectionResources[plugin] = string(jsonByte)
		}
	}

	result.Sources = srcResources
	result.Sinks = sinkResources
	result.Connections = connectionResources

	return result
}

type YamlConfigurationKeys struct {
	Sources map[string][]string
	Sinks   map[string][]string
}

func GetConfigurationsFor(yaml YamlConfigurationKeys) YamlConfigurationSet {
	ConfigManager.lock.RLock()
	defer ConfigManager.lock.RUnlock()

	sourcesConfigKeys := yaml.Sources
	sinksConfigKeys := yaml.Sinks

	result := YamlConfigurationSet{
		Sources:     map[string]string{},
		Sinks:       map[string]string{},
		Connections: map[string]string{},
	}
	srcResources := map[string]string{}
	sinkResources := map[string]string{}
	connectionResources := map[string]string{}

	for key, ops := range ConfigManager.cfgOperators {
		if strings.HasPrefix(key, SourceCfgOperatorKeyPrefix) {
			plugin := strings.TrimPrefix(key, SourceCfgOperatorKeyPrefix)
			keys, ok := sourcesConfigKeys[plugin]
			if ok {
				cfs := ops.CopyUpdatableConfContentFor(keys)
				if len(cfs) > 0 {
					jsonByte, _ := json.Marshal(cfs)
					srcResources[plugin] = string(jsonByte)
				}
			}
			continue
		}
		if strings.HasPrefix(key, SinkCfgOperatorKeyPrefix) {
			plugin := strings.TrimPrefix(key, SinkCfgOperatorKeyPrefix)
			keys, ok := sinksConfigKeys[plugin]
			if ok {
				cfs := ops.CopyUpdatableConfContentFor(keys)
				if len(cfs) > 0 {
					jsonByte, _ := json.Marshal(cfs)
					sinkResources[plugin] = string(jsonByte)
				}
			}
			continue
		}
	}
	connConfigs, err := conf.GetAllConnConfigs()
	if err != nil {
		conf.Log.Warnf("export connections err:%v", err)
	} else {
		for plugin, props := range connConfigs {
			jsonByte, _ := json.Marshal(props)
			connectionResources[plugin] = string(jsonByte)
		}
	}

	result.Sources = srcResources
	result.Sinks = sinkResources
	result.Connections = connectionResources

	return result
}

func GetConfigurationStatus() YamlConfigurationSet {
	result := YamlConfigurationSet{
		Sources:     map[string]string{},
		Sinks:       map[string]string{},
		Connections: map[string]string{},
	}

	all, err := ConfigManager.sourceConfigStatusDb.All()
	if err == nil {
		result.Sources = all
	}

	all, err = ConfigManager.sinkConfigStatusDb.All()
	if err == nil {
		result.Sinks = all
	}

	all, err = ConfigManager.connectionConfigStatusDb.All()
	if err == nil {
		result.Connections = all
	}

	return result
}

func LoadConfigurations(configSets YamlConfigurationSet) YamlConfigurationSet {
	configResponse := YamlConfigurationSet{
		Sources:     map[string]string{},
		Sinks:       map[string]string{},
		Connections: map[string]string{},
	}

	srcResources := configSets.Sources
	sinkResources := configSets.Sinks
	connectionResources := configSets.Connections

	_ = ConfigManager.sourceConfigStatusDb.Clean()
	_ = ConfigManager.sinkConfigStatusDb.Clean()
	_ = ConfigManager.connectionConfigStatusDb.Clean()

	for key, val := range srcResources {
		configs := YamlConfigurations{}
		err := json.Unmarshal(cast.StringToBytes(val), &configs)
		if err != nil {
			_ = ConfigManager.sourceConfigStatusDb.Set(key, err.Error())
			configResponse.Sources[key] = err.Error()
			continue
		}
		err = addSourceConfKeys(key, configs)
		if err != nil {
			_ = ConfigManager.sourceConfigStatusDb.Set(key, err.Error())
			configResponse.Sources[key] = err.Error()
			continue
		}
	}

	for key, val := range sinkResources {
		configs := YamlConfigurations{}
		err := json.Unmarshal(cast.StringToBytes(val), &configs)
		if err != nil {
			_ = ConfigManager.sinkConfigStatusDb.Set(key, err.Error())
			configResponse.Sinks[key] = err.Error()
			continue
		}
		err = addSinkConfKeys(key, configs)
		if err != nil {
			_ = ConfigManager.sinkConfigStatusDb.Set(key, err.Error())
			configResponse.Sinks[key] = err.Error()
			continue
		}
	}

	for key, val := range connectionResources {
		configs := YamlConfigurations{}
		err := json.Unmarshal(cast.StringToBytes(val), &configs)
		if err != nil {
			_ = ConfigManager.connectionConfigStatusDb.Set(key, err.Error())
			configResponse.Connections[key] = err.Error()
			continue
		}
		err = addConnectionConfKeys(key, configs)
		if err != nil {
			_ = ConfigManager.connectionConfigStatusDb.Set(key, err.Error())
			configResponse.Connections[key] = err.Error()
			continue
		}
	}
	if err := connection.ReloadNamedConnection(); err != nil {
		conf.Log.Errorf("reload connection config error: %s", err.Error())
	}
	return configResponse
}

func LoadConfigurationsPartial(configSets YamlConfigurationSet) YamlConfigurationSet {
	configResponse := YamlConfigurationSet{
		Sources:     map[string]string{},
		Sinks:       map[string]string{},
		Connections: map[string]string{},
	}

	srcResources := configSets.Sources
	sinkResources := configSets.Sinks
	connectionResources := configSets.Connections

	for key, val := range srcResources {
		configs := YamlConfigurations{}
		err := json.Unmarshal(cast.StringToBytes(val), &configs)
		if err != nil {
			configResponse.Sources[key] = err.Error()
			continue
		}
		err = addSourceConfKeys(key, configs)
		if err != nil {
			configResponse.Sources[key] = err.Error()
			continue
		}
	}

	for key, val := range sinkResources {
		configs := YamlConfigurations{}
		err := json.Unmarshal(cast.StringToBytes(val), &configs)
		if err != nil {
			configResponse.Sinks[key] = err.Error()
			continue
		}
		err = addSinkConfKeys(key, configs)
		if err != nil {
			configResponse.Sinks[key] = err.Error()
			continue
		}
	}

	for key, val := range connectionResources {
		configs := YamlConfigurations{}
		err := json.Unmarshal(cast.StringToBytes(val), &configs)
		if err != nil {
			configResponse.Connections[key] = err.Error()
			continue
		}
		err = addConnectionConfKeys(key, configs)
		if err != nil {
			configResponse.Connections[key] = err.Error()
			continue
		}
	}
	if err := connection.ReloadNamedConnection(); err != nil {
		conf.Log.Errorf("reload connection config error: %s", err.Error())
	}
	return configResponse
}
