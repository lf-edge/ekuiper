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
	"strings"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/pkg/ast"
	"github.com/lf-edge/ekuiper/v2/pkg/connection"
)

// GetSourceConf unifies all properties set in different locations
func GetSourceConf(sourceType string, options *ast.Options) map[string]interface{} {
	confkey := options.CONF_KEY

	yamlOps, err := conf.NewConfigOperatorFromSourceStorage(sourceType)
	if err != nil {
		conf.Log.Warnf("fail to parse yaml for source %s. Return error %v", sourceType, err)
	}
	props := make(map[string]interface{})
	cfg := yamlOps.CopyConfContent()
	if len(cfg) == 0 {
		conf.Log.Warnf("fail to parse yaml for source %s. Return an empty configuration", sourceType)
	} else {
		def, ok := cfg["default"]
		if !ok {
			conf.Log.Warnf("default config_key not found")
		} else {
			props = def
		}
		if confkey != "" {
			// config keys in etc folder will transform to lowercase
			// while those in data will not
			if c, ok := cfg[strings.ToLower(confkey)]; ok {
				for k, v := range c {
					props[k] = v
				}
			} else if c, ok := cfg[confkey]; ok {
				for k, v := range c {
					props[k] = v
				}
			} else {
				conf.Log.Warnf("fail to find config key %s for source %s", confkey, sourceType)
			}
		}
	}
	connectionSelector, ok := props["connectionSelector"]
	if ok {
		selectorID, ok := connectionSelector.(string)
		if ok {
			meta, err := connection.GetConnectionDetail(nil, selectorID)
			if err != nil {
				conf.Log.Warnf("load connection meta %s failed, err:%v", selectorID, err)
			} else {
				for key, value := range meta.Props {
					props[key] = value
				}
			}
		}
	}

	if options.EXTRA != "" {
		err = json.Unmarshal([]byte(options.EXTRA), &props)
		if err != nil {
			conf.Log.Warnf("load extra option %s failed, err:%v", options.EXTRA, err)
		}
	}
	f := options.FORMAT
	if f == "" {
		f = "json"
	}
	props["format"] = strings.ToLower(f)
	props["key"] = options.KEY
	props["datasource"] = options.DATASOURCE
	props["schemaId"] = options.SCHEMAID
	props["delimiter"] = options.DELIMITER
	props["retainSize"] = options.RETAIN_SIZE
	props["strictValidation"] = options.STRICT_VALIDATION
	props["timestamp"] = options.TIMESTAMP
	props["timestampFormat"] = options.TIMESTAMP_FORMAT
	conf.Log.Infof("get conf for %s with conf key %s: %v", sourceType, confkey, printable(props))
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
