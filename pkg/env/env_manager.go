// Copyright 2024 EMQ Technologies Co., Ltd.
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

package env

import (
	"fmt"
	"os"
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

func init() {
	globalEnvManager = &EnvManager{}
}

func Setup() {
	globalEnvManager.Setup()
}

var globalEnvManager *EnvManager

type EnvManager struct {
	env             map[string]string
	connectionProps map[string]map[string]map[string]interface{}
}

func (e *EnvManager) Setup() {
	e.loadEnv()
	e.loadConnectionProps()
	e.storeConnectionProps()
	// clear useless data
	e.clear()
}

func (e *EnvManager) loadEnv() {
	got := os.Environ()
	e.env = make(map[string]string)
	for _, v := range got {
		ss := strings.Split(v, "=")
		if len(ss) != 2 {
			continue
		}
		key := ss[0]
		value := ss[1]
		if strings.HasPrefix(key, "CONNECTION") {
			e.env[key] = value
		}
	}
}

func (e *EnvManager) loadConnectionProps() {
	e.connectionProps = make(map[string]map[string]map[string]interface{})
	for k, v := range e.env {
		ss := strings.Split(k, "__")
		if len(ss) != 4 {
			continue
		}
		pluginTyp := strings.ToLower(ss[1])
		confName := strings.ToLower(ss[2])
		confKey := strings.ToLower(ss[3])
		v1, ok := e.connectionProps[pluginTyp]
		if !ok {
			v1 = make(map[string]map[string]interface{})
			e.connectionProps[pluginTyp] = v1
		}
		v2, ok := v1[confName]
		if !ok {
			v2 = make(map[string]interface{})
			v1[confName] = v2
		}
		v2[confKey] = v
	}
}

func (e *EnvManager) storeConnectionProps() {
	for pluginTyp, v := range e.connectionProps {
		for confName, props := range v {
			err := conf.WriteCfgIntoKVStorage("connections", pluginTyp, confName, props)
			if err != nil {
				conf.Log.Warn(fmt.Sprintf("load connections.%s.%s failed, err:%v", pluginTyp, confName, err))
			}
		}
	}
}

func (e *EnvManager) clear() {
	e.env = nil
	e.connectionProps = nil
}
