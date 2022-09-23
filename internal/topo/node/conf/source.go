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

package conf

import (
	"github.com/lf-edge/ekuiper/internal/conf"
	"github.com/lf-edge/ekuiper/pkg/ast"
	"strings"
)

func GetSourceConf(sourceType string, options *ast.Options) map[string]interface{} {
	confkey := options.CONF_KEY
	confPath := "sources/" + sourceType + ".yaml"
	if sourceType == "mqtt" {
		confPath = "mqtt_source.yaml"
	}
	props := make(map[string]interface{})
	cfg := make(map[string]interface{})
	err := conf.LoadConfigByName(confPath, &cfg)
	if err != nil {
		conf.Log.Warnf("fail to parse yaml for source %s. Return an empty configuration", sourceType)
	} else {
		def, ok := cfg["default"]
		if !ok {
			conf.Log.Warnf("default conf %s is not found", confkey)
		} else {
			if def1, ok1 := def.(map[string]interface{}); ok1 {
				props = def1
			}
			if c, ok := cfg[strings.ToLower(confkey)]; ok {
				if c1, ok := c.(map[string]interface{}); ok {
					c2 := c1
					for k, v := range c2 {
						props[k] = v
					}
				}
			}
		}
	}
	f := options.FORMAT
	if f == "" {
		f = "json"
	}
	props["format"] = strings.ToLower(f)
	conf.Log.Debugf("get conf for %s with conf key %s: %v", sourceType, confkey, printable(props))
	return props
}

func printable(m map[string]interface{}) map[string]interface{} {
	printableMap := make(map[string]interface{})
	for k, v := range m {
		if strings.EqualFold(k, "password") {
			printableMap[k] = "*"
		} else {
			if vm, ok := v.(map[string]interface{}); ok {
				printableMap[k] = printable(vm)
			} else {
				printableMap[k] = v
			}
		}
	}
	return printableMap
}
