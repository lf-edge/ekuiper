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

package conf

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

func init() {
	globalEnvManager = &EnvManager{}
	SetupEnv()
}

func SetupEnv() {
	globalEnvManager.Setup()
}

func SetupConnectionProps() {
	globalEnvManager.SetupConnectionProps()
}

func GetEnv() map[string]string {
	return globalEnvManager.GetEnv()
}

var globalEnvManager *EnvManager

type EnvManager struct {
	env             map[string]string
	connectionProps map[string]map[string]map[string]interface{}
}

func (e *EnvManager) Setup() {
	e.loadEnv()
}

func (e *EnvManager) GetEnv() map[string]string {
	if len(e.env) < 1 {
		e.loadEnv()
	}
	return e.env
}

func (e *EnvManager) SetupConnectionProps() {
	e.loadConnectionProps()
	e.storeConnectionProps()
	e.connectionProps = nil
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
		e.env[key] = value
	}
}

func (e *EnvManager) loadConnectionProps() {
	e.connectionProps = make(map[string]map[string]map[string]interface{})
	for k, v := range e.env {
		if !strings.HasPrefix(k, "CONNECTION") {
			continue
		}
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
		v2[confKey] = parseValue(v)
	}
}

func (e *EnvManager) storeConnectionProps() {
	for pluginTyp, v := range e.connectionProps {
		for confName, props := range v {
			err := WriteCfgIntoKVStorage("connections", pluginTyp, confName, props)
			if err != nil {
				Log.Warn(fmt.Sprintf("load connections.%s.%s failed, err:%v", pluginTyp, confName, err))
			}
		}
	}
}

func parseValue(v interface{}) interface{} {
	sd, ok := v.(string)
	if !ok {
		return sd
	}
	iv, err := strconv.ParseInt(sd, 10, 64)
	if err == nil {
		return iv
	}
	fv, err := strconv.ParseFloat(sd, 64)
	if err == nil {
		return fv
	}
	bv, err := strconv.ParseBool(sd)
	if err == nil {
		return bv
	}
	return sd
}
