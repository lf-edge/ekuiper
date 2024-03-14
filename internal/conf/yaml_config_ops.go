// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

package conf

import (
	"encoding/json"
	"fmt"
	"path"
	"reflect"
	"sync"

	"github.com/lf-edge/ekuiper/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/pkg/cast"
)

// ConfKeysOperator define interface to query/add/update/delete the configs in memory
type ConfKeysOperator interface {
	GetPluginName() string
	GetConfContentByte() ([]byte, error)
	// CopyConfContent get the configurations in etc and data folder
	CopyConfContent() map[string]map[string]interface{}
	// CopyReadOnlyConfContent get the configurations in etc folder
	CopyReadOnlyConfContent() map[string]map[string]interface{}
	// CopyUpdatableConfContent get the configurations in data folder
	CopyUpdatableConfContent() map[string]map[string]interface{}
	// CopyUpdatableConfContentFor get the configuration for the specific configKeys
	CopyUpdatableConfContentFor(configKeys []string) map[string]map[string]interface{}
	// LoadConfContent load the configurations into data configuration part
	LoadConfContent(cf map[string]map[string]interface{})
	GetConfKeys() (keys []string)
	GetReadOnlyConfKeys() (keys []string)
	GetUpdatableConfKeys() (keys []string)
	DeleteConfKey(confKey string)
	DeleteConfKeyField(confKey string, reqField map[string]interface{}) error
	AddConfKey(confKey string, reqField map[string]interface{}) error
	AddConfKeyField(confKey string, reqField map[string]interface{}) error
	ClearConfKeys()
}

// ConfigOperator define interface to query/add/update/delete the configs in disk
type ConfigOperator interface {
	ConfKeysOperator
	SaveCfgToStorage() error
}

// ConfigKeys implement ConfKeysOperator interface, load the configs from etc/sources/xx.yaml and et/connections/connection.yaml
// Hold the connection configs for each connection type in etcCfg field
// Provide method to query/add/update/delete the configs
type ConfigKeys struct {
	storageType string
	lock        sync.RWMutex
	pluginName  string                            // source type, can be mqtt/edgex/httppull
	etcCfg      map[string]map[string]interface{} // configs defined in etc/sources/yaml
	dataCfg     map[string]map[string]interface{} // configs defined in etc/sources/
	// delCfgKey save the config key which needs to be deleted from the storage
	delCfgKey map[string]struct{}
	// saveCfgKey save the config key which needs to be saved or updated into the storage
	saveCfgKey map[string]struct{}
}

func (c *ConfigKeys) saveCfgKeysIntoKVStorage(cfgType string) error {
	for key := range c.saveCfgKey {
		if err := saveCfgKeyToKV(buildKey(cfgType, c.pluginName, key), c.dataCfg[key]); err != nil {
			return err
		}
		delete(c.saveCfgKey, key)
	}
	for key := range c.delCfgKey {
		if err := delCfgKeyInStorage(buildKey(cfgType, c.pluginName, key)); err != nil {
			return err
		}
		delete(c.delCfgKey, key)
	}
	return nil
}

func (c *ConfigKeys) GetPluginName() string {
	return c.pluginName
}

func (c *ConfigKeys) GetConfContentByte() ([]byte, error) {
	cf := make(map[string]map[string]interface{})
	c.lock.RLock()
	defer c.lock.RUnlock()
	for key, kvs := range c.etcCfg {
		aux := make(map[string]interface{})
		for k, v := range kvs {
			aux[k] = v
		}
		cf[key] = aux
	}

	for key, kvs := range c.dataCfg {
		aux := make(map[string]interface{})
		for k, v := range kvs {
			aux[k] = v
		}
		cf[key] = aux
	}

	return json.Marshal(cf)
}

func (c *ConfigKeys) CopyConfContent() map[string]map[string]interface{} {
	cf := make(map[string]map[string]interface{})
	c.lock.RLock()
	defer c.lock.RUnlock()

	for key, kvs := range c.etcCfg {
		aux := make(map[string]interface{})
		for k, v := range kvs {
			aux[k] = v
		}
		cf[key] = aux
	}

	// note: config keys in data directory will overwrite those in etc directory with same name
	for key, kvs := range c.dataCfg {
		aux := make(map[string]interface{})
		for k, v := range kvs {
			aux[k] = v
		}
		cf[key] = aux
	}

	return cf
}

func (c *ConfigKeys) LoadConfContent(cf map[string]map[string]interface{}) {
	c.lock.Lock()
	defer c.lock.Unlock()

	for key, kvs := range cf {
		aux := make(map[string]interface{})
		for k, v := range kvs {
			aux[k] = v
		}
		c.dataCfg[key] = aux
	}
}

func (c *ConfigKeys) CopyReadOnlyConfContent() map[string]map[string]interface{} {
	cf := make(map[string]map[string]interface{})
	c.lock.RLock()
	defer c.lock.RUnlock()

	for key, kvs := range c.etcCfg {
		aux := make(map[string]interface{})
		for k, v := range kvs {
			aux[k] = v
		}
		cf[key] = aux
	}

	return cf
}

func (c *ConfigKeys) CopyUpdatableConfContent() map[string]map[string]interface{} {
	cf := make(map[string]map[string]interface{})
	c.lock.RLock()
	defer c.lock.RUnlock()

	for key, kvs := range c.dataCfg {
		aux := make(map[string]interface{})
		for k, v := range kvs {
			aux[k] = v
		}
		cf[key] = aux
	}

	return cf
}

func (c *ConfigKeys) CopyUpdatableConfContentFor(configKeys []string) map[string]map[string]interface{} {
	cf := make(map[string]map[string]interface{})
	c.lock.RLock()
	defer c.lock.RUnlock()

	for _, key := range configKeys {
		if key == "" {
			key = "default"
		}
		if kvs, ok := c.dataCfg[key]; ok {
			aux := make(map[string]interface{})
			for k, v := range kvs {
				aux[k] = v
			}
			cf[key] = aux
		}
	}
	return cf
}

func (c *ConfigKeys) GetConfKeys() (keys []string) {
	ro := c.GetReadOnlyConfKeys()
	keys = append(keys, ro...)

	up := c.GetUpdatableConfKeys()
	keys = append(keys, up...)

	return keys
}

func (c *ConfigKeys) GetReadOnlyConfKeys() (keys []string) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for k := range c.etcCfg {
		keys = append(keys, k)
	}
	return keys
}

func (c *ConfigKeys) GetUpdatableConfKeys() (keys []string) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for k := range c.dataCfg {
		keys = append(keys, k)
	}
	return keys
}

func (c *ConfigKeys) DeleteConfKey(confKey string) {
	c.lock.Lock()
	defer c.lock.Unlock()
	delete(c.dataCfg, confKey)
	c.delCfgKey[confKey] = struct{}{}
}

func (c *ConfigKeys) ClearConfKeys() {
	keys := c.GetUpdatableConfKeys()
	for _, key := range keys {
		c.DeleteConfKey(key)
	}
}

func recursionDelMap(cf, fields map[string]interface{}) error {
	for k, v := range fields {
		if nil == v {
			delete(cf, k)
			continue
		}

		if delKey, ok := v.(string); ok {
			if 0 == len(delKey) {
				delete(cf, k)
				continue
			}
			var auxCf map[string]interface{}
			if err := cast.MapToStruct(cf[k], &auxCf); nil != err {
				return fmt.Errorf(`%s%s.%s`, "type_conversion_fail", k, delKey)
			}
			cf[k] = auxCf
			delete(auxCf, delKey)
			continue
		}
		if reflect.TypeOf(v) != nil && reflect.Map == reflect.TypeOf(v).Kind() {
			var auxCf, auxFields map[string]interface{}
			if err := cast.MapToStruct(cf[k], &auxCf); nil != err {
				return fmt.Errorf(`%s%s.%v`, "type_conversion_fail", k, v)
			}
			cf[k] = auxCf
			if err := cast.MapToStruct(v, &auxFields); nil != err {
				return fmt.Errorf(`%s%s.%v`, "type_conversion_fail", k, v)
			}
			if err := recursionDelMap(auxCf, auxFields); nil != err {
				return err
			}
		}
	}
	return nil
}

func (c *ConfigKeys) DeleteConfKeyField(confKey string, reqField map[string]interface{}) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	err := recursionDelMap(c.dataCfg[confKey], reqField)
	if nil != err {
		return err
	}

	return nil
}

func (c *ConfigKeys) AddConfKey(confKey string, reqField map[string]interface{}) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	c.dataCfg[confKey] = reqField
	c.saveCfgKey[confKey] = struct{}{}
	return nil
}

func (c *ConfigKeys) AddConfKeyField(confKey string, reqField map[string]interface{}) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if nil == c.dataCfg[confKey] {
		return fmt.Errorf(`%s`, "not_found_confkey")
	}

	for k, v := range reqField {
		c.dataCfg[confKey][k] = v
	}
	return nil
}

// SourceConfigKeysOps implement ConfOperator interface, load the configs from etc/sources/xx.yaml
type SourceConfigKeysOps struct {
	*ConfigKeys
}

func (c *SourceConfigKeysOps) SaveCfgToStorage() error {
	switch c.storageType {
	case cfgStoreKVStorage:
		return c.ConfigKeys.saveCfgKeysIntoKVStorage("sources")
	case cfgFileStorage:
		pluginName := c.pluginName
		confDir, err := GetDataLoc()
		if nil != err {
			return err
		}

		dir := path.Join(confDir, "sources")
		filePath := path.Join(dir, pluginName+".yaml")
		cfg := c.CopyUpdatableConfContent()
		err = filex.WriteYamlMarshal(filePath, cfg)
		return err
	}
	return fmt.Errorf("unknown source cfg storage type: %v", c.storageType)
}

// SinkConfigKeysOps implement ConfOperator interface, load the configs from data/sinks/xx.yaml
type SinkConfigKeysOps struct {
	*ConfigKeys
}

func (c *SinkConfigKeysOps) SaveCfgToStorage() error {
	switch c.storageType {
	case cfgStoreKVStorage:
		return c.ConfigKeys.saveCfgKeysIntoKVStorage("sinks")
	case cfgFileStorage:
		pluginName := c.pluginName
		confDir, err := GetDataLoc()
		if nil != err {
			return err
		}

		dir := path.Join(confDir, "sinks")
		filePath := path.Join(dir, pluginName+".yaml")
		cfg := c.CopyUpdatableConfContent()
		err = filex.WriteYamlMarshal(filePath, cfg)
		return err
	}
	return fmt.Errorf("unknown sinks cfg storage type: %v", c.storageType)
}

// ConnectionConfigKeysOps implement ConfOperator interface, load the configs from et/connections/connection.yaml
type ConnectionConfigKeysOps struct {
	*ConfigKeys
}

func (p *ConnectionConfigKeysOps) SaveCfgToStorage() error {
	switch p.storageType {
	case cfgStoreKVStorage:
		return p.ConfigKeys.saveCfgKeysIntoKVStorage("connections")
	case cfgFileStorage:
		pluginName := p.pluginName
		confDir, err := GetDataLoc()
		if nil != err {
			return err
		}

		cfg := p.CopyUpdatableConfContent()

		yamlPath := path.Join(confDir, "connections/connection.yaml")

		yamlData := make(map[string]interface{})
		err = filex.ReadYamlUnmarshal(yamlPath, &yamlData)
		if nil != err {
			return err
		}
		yamlData[pluginName] = cfg
		return filex.WriteYamlMarshal(yamlPath, yamlData)
	}
	return fmt.Errorf("unknown connection cfg storage type: %v", p.ConfigKeys.storageType)
}

// NewConfigOperatorForSource construct function
func NewConfigOperatorForSource(pluginName string) ConfigOperator {
	c := &SourceConfigKeysOps{
		&ConfigKeys{
			storageType: getStorageType(),
			lock:        sync.RWMutex{},
			pluginName:  pluginName,
			etcCfg:      map[string]map[string]interface{}{},
			dataCfg:     map[string]map[string]interface{}{},
			delCfgKey:   map[string]struct{}{},
			saveCfgKey:  map[string]struct{}{},
		},
	}
	return c
}

// NewConfigOperatorFromSourceStorage construct function, Load the configs from etc/sources/xx.yaml
func NewConfigOperatorFromSourceStorage(pluginName string) (ConfigOperator, error) {
	c := &SourceConfigKeysOps{
		&ConfigKeys{
			storageType: getStorageType(),
			lock:        sync.RWMutex{},
			pluginName:  pluginName,
			etcCfg:      map[string]map[string]interface{}{},
			dataCfg:     map[string]map[string]interface{}{},
			delCfgKey:   map[string]struct{}{},
			saveCfgKey:  map[string]struct{}{},
		},
	}

	confDir, err := GetConfLoc()
	if nil != err {
		return nil, err
	}
	dir := path.Join(confDir, "sources")
	fileName := pluginName
	if "mqtt" == pluginName {
		fileName = "mqtt_source"
		dir = confDir
	}
	filePath := path.Join(dir, fileName+`.yaml`)
	// Just ignore error if yaml not found
	_ = LoadConfigFromPath(filePath, &c.etcCfg)

	if c.storageType != cfgFileStorage {
		prefix := buildKey("sources", pluginName, "")
		dataCfg, err := getCfgKeyFromStorageByPrefix(prefix)
		if err != nil {
			return nil, err
		}
		c.dataCfg = dataCfg
	} else {
		dataDir, err := GetDataLoc()
		if nil != err {
			return nil, err
		}
		dir = path.Join(dataDir, "sources")
		fileName = pluginName

		filePath = path.Join(dir, fileName+`.yaml`)
		_ = filex.ReadYamlUnmarshal(filePath, &c.dataCfg)
	}
	return c, nil
}

// NewConfigOperatorForSink construct function
func NewConfigOperatorForSink(pluginName string) ConfigOperator {
	c := &SinkConfigKeysOps{
		&ConfigKeys{
			storageType: getStorageType(),
			lock:        sync.RWMutex{},
			pluginName:  pluginName,
			etcCfg:      map[string]map[string]interface{}{},
			dataCfg:     map[string]map[string]interface{}{},
			delCfgKey:   map[string]struct{}{},
			saveCfgKey:  map[string]struct{}{},
		},
	}
	return c
}

// NewConfigOperatorFromSinkStorage construct function, Load the configs from etc/sources/xx.yaml
func NewConfigOperatorFromSinkStorage(pluginName string) (ConfigOperator, error) {
	c := &SinkConfigKeysOps{
		&ConfigKeys{
			storageType: getStorageType(),
			lock:        sync.RWMutex{},
			pluginName:  pluginName,
			etcCfg:      map[string]map[string]interface{}{},
			dataCfg:     map[string]map[string]interface{}{},
			delCfgKey:   map[string]struct{}{},
			saveCfgKey:  map[string]struct{}{},
		},
	}
	if c.storageType != cfgFileStorage {
		prefix := buildKey("sinks", pluginName, "")
		dataCfg, err := getCfgKeyFromStorageByPrefix(prefix)
		if err != nil {
			return nil, err
		}
		c.dataCfg = dataCfg
	} else {
		dataDir, err := GetDataLoc()
		if nil != err {
			return nil, err
		}
		dir := path.Join(dataDir, "sinks")

		filePath := path.Join(dir, pluginName+`.yaml`)
		_ = filex.ReadYamlUnmarshal(filePath, &c.dataCfg)
	}
	return c, nil
}

// NewConfigOperatorForConnection construct function
func NewConfigOperatorForConnection(pluginName string) ConfigOperator {
	c := &ConnectionConfigKeysOps{
		&ConfigKeys{
			storageType: getStorageType(),
			lock:        sync.RWMutex{},
			pluginName:  pluginName,
			etcCfg:      map[string]map[string]interface{}{},
			dataCfg:     map[string]map[string]interface{}{},
			delCfgKey:   map[string]struct{}{},
			saveCfgKey:  map[string]struct{}{},
		},
	}
	return c
}

// NewConfigOperatorFromConnectionStorage construct function, Load the configs from et/connections/connection.yaml
func NewConfigOperatorFromConnectionStorage(pluginName string) (ConfigOperator, error) {
	c := &ConnectionConfigKeysOps{
		&ConfigKeys{
			storageType: getStorageType(),
			lock:        sync.RWMutex{},
			pluginName:  pluginName,
			etcCfg:      map[string]map[string]interface{}{},
			dataCfg:     map[string]map[string]interface{}{},
			delCfgKey:   map[string]struct{}{},
			saveCfgKey:  map[string]struct{}{},
		},
	}

	confDir, err := GetConfLoc()
	if nil != err {
		return nil, err
	}
	yamlPath := path.Join(confDir, "connections/connection.yaml")
	yamlData := make(map[string]interface{})
	err = LoadConfigFromPath(yamlPath, &yamlData)
	if nil != err {
		return nil, err
	}
	if plgCnfs, ok := yamlData[pluginName]; ok {
		if cf, ok1 := plgCnfs.(map[string]interface{}); ok1 {
			for confKey, confVal := range cf {
				if conf, ok := confVal.(map[string]interface{}); ok {
					c.etcCfg[confKey] = conf
				} else {
					return nil, fmt.Errorf("file content is not right: %s.%v", confKey, confVal)
				}
			}
		} else {
			return nil, fmt.Errorf("file content is not right: %v", plgCnfs)
		}
	} else {
		return nil, fmt.Errorf("not find the target connection type: %s", c.pluginName)
	}

	if c.storageType != cfgFileStorage {
		prefix := buildKey("connections", pluginName, "")
		dataCfg, err := getCfgKeyFromStorageByPrefix(prefix)
		if err != nil {
			return nil, err
		}
		c.dataCfg = dataCfg
	} else {
		confDir, err = GetDataLoc()
		if nil != err {
			return nil, err
		}
		yamlPath = path.Join(confDir, "connections/connection.yaml")
		yamlData = make(map[string]interface{})
		_ = filex.ReadYamlUnmarshal(yamlPath, &yamlData)

		if plgCnfs, ok := yamlData[pluginName]; ok {
			if cf, ok1 := plgCnfs.(map[string]interface{}); ok1 {
				for confKey, confVal := range cf {
					if conf, ok := confVal.(map[string]interface{}); ok {
						c.dataCfg[confKey] = conf
					} else {
						return nil, fmt.Errorf("file content is not right: %s.%v", confKey, confVal)
					}
				}
			} else {
				return nil, fmt.Errorf("file content is not right: %v", plgCnfs)
			}
		}
	}
	return c, nil
}

func getStorageType() string {
	st := cfgFileStorage
	if Config != nil && len(Config.Basic.CfgStorageType) > 0 {
		st = Config.Basic.CfgStorageType
	}
	return st
}
