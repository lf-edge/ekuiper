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
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/internal/pkg/filex"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"path"
	"reflect"
	"sync"
)

//ConfKeysOperator define interface to query/add/update/delete the configs in memory
type ConfKeysOperator interface {
	GetPluginName() string
	GetConfContentByte() ([]byte, error)
	CopyConfContent() map[string]map[string]interface{}
	GetConfKeys() (keys []string)
	DeleteConfKey(confKey string)
	DeleteConfKeyField(confKey string, reqField map[string]interface{}) error
	AddConfKey(confKey string, reqField map[string]interface{}) error
	AddConfKeyField(confKey string, reqField map[string]interface{}) error
}

//ConfigOperator define interface to query/add/update/delete the configs in disk
type ConfigOperator interface {
	ConfKeysOperator
	IsSource() bool
	SaveCfgToFile() error
}

// ConfigKeys implement ConfKeysOperator interface, load the configs from etc/sources/xx.yaml and et/connections/connection.yaml
// Hold the connection configs for each connection type in cf field
// Provide method to query/add/update/delete the configs
type ConfigKeys struct {
	lock       sync.RWMutex
	pluginName string                            // source type, can be mqtt/edgex/httppull
	cf         map[string]map[string]interface{} // configs defined in yaml
}

func (c *ConfigKeys) GetPluginName() string {
	return c.pluginName
}

func (c *ConfigKeys) GetConfContentByte() ([]byte, error) {
	cf := make(map[string]map[string]interface{})
	c.lock.RLock()
	defer c.lock.RUnlock()
	for key, kvs := range c.cf {
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

	for key, kvs := range c.cf {
		aux := make(map[string]interface{})
		for k, v := range kvs {
			aux[k] = v
		}
		cf[key] = aux
	}

	return cf
}

func (c *ConfigKeys) GetConfKeys() (keys []string) {
	c.lock.RLock()
	defer c.lock.RUnlock()

	for k := range c.cf {
		keys = append(keys, k)
	}
	return keys
}

func (c *ConfigKeys) DeleteConfKey(confKey string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	delete(c.cf, confKey)
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

	err := recursionDelMap(c.cf[confKey], reqField)
	if nil != err {
		return err
	}

	return nil
}

func (c *ConfigKeys) AddConfKey(confKey string, reqField map[string]interface{}) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.cf[confKey] = reqField

	return nil
}

func (c *ConfigKeys) AddConfKeyField(confKey string, reqField map[string]interface{}) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if nil == c.cf[confKey] {
		return fmt.Errorf(`%s`, "not_found_confkey")
	}

	for k, v := range reqField {
		c.cf[confKey][k] = v
	}
	return nil
}

// SourceConfigKeysOps implement ConfOperator interface, load the configs from etc/sources/xx.yaml
type SourceConfigKeysOps struct {
	*ConfigKeys
}

func (c *SourceConfigKeysOps) IsSource() bool {
	return true
}

func (c *SourceConfigKeysOps) SaveCfgToFile() error {
	pluginName := c.pluginName
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return err
	}

	dir := path.Join(confDir, "sources")
	if "mqtt" == pluginName {
		pluginName = "mqtt_source"
		dir = confDir
	}
	filePath := path.Join(dir, pluginName+".yaml")
	cfg := c.CopyConfContent()
	err = filex.WriteYamlMarshal(filePath, cfg)
	if nil != err {
		return err
	}
	return nil
}

// ConnectionConfigKeysOps implement ConfOperator interface, load the configs from et/connections/connection.yaml
type ConnectionConfigKeysOps struct {
	*ConfigKeys
}

func (p *ConnectionConfigKeysOps) IsSource() bool {
	return false
}

func (p *ConnectionConfigKeysOps) SaveCfgToFile() error {
	pluginName := p.pluginName
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return err
	}

	cfg := p.CopyConfContent()

	yamlPath := path.Join(confDir, "connections/connection.yaml")

	yamlData := make(map[string]interface{})
	err = filex.ReadYamlUnmarshal(yamlPath, &yamlData)
	if nil != err {
		return err
	}

	yamlData[pluginName] = cfg

	return filex.WriteYamlMarshal(yamlPath, yamlData)
}

// NewConfigOperatorFromSourceYaml construct function, Load the configs from etc/sources/xx.yaml
func NewConfigOperatorFromSourceYaml(pluginName string) (ConfigOperator, error) {
	confDir, err := conf.GetConfLoc()
	if nil != err {
		return nil, err
	}

	c := &SourceConfigKeysOps{
		&ConfigKeys{
			lock:       sync.RWMutex{},
			pluginName: pluginName,
			cf:         map[string]map[string]interface{}{},
		},
	}

	dir := path.Join(confDir, "sources")
	fileName := pluginName
	if "mqtt" == pluginName {
		fileName = "mqtt_source"
		dir = confDir
	}
	filePath := path.Join(dir, fileName+`.yaml`)
	err = filex.ReadYamlUnmarshal(filePath, &c.cf)
	if nil != err {
		return nil, err
	}

	return c, nil
}

// NewConfigOperatorFromConnectionYaml construct function, Load the configs from et/connections/connection.yaml
func NewConfigOperatorFromConnectionYaml(pluginName string) (ConfigOperator, error) {

	confDir, err := conf.GetConfLoc()
	if nil != err {
		return nil, err
	}

	yamlPath := path.Join(confDir, "connections/connection.yaml")
	yamlData := make(map[string]interface{})
	err = filex.ReadYamlUnmarshal(yamlPath, &yamlData)
	if nil != err {
		return nil, err
	}

	c := &ConnectionConfigKeysOps{
		&ConfigKeys{
			lock:       sync.RWMutex{},
			pluginName: pluginName,
			cf:         map[string]map[string]interface{}{},
		},
	}

	if plgCnfs, ok := yamlData[pluginName]; ok {
		if cf, ok1 := plgCnfs.(map[string]interface{}); ok1 {
			for confKey, confVal := range cf {
				if conf, ok := confVal.(map[string]interface{}); ok {
					c.cf[confKey] = conf
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

	return c, nil
}
