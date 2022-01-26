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
	"fmt"
	"strings"
)

const CONNECTION_CONF = "connections/connection.yaml"

type ConSelector struct {
	ConnSelectorStr string
	Type            string // mqtt edgex
	CfgKey          string // config key
}

func (c *ConSelector) Init() error {
	conTypeSel := strings.SplitN(c.ConnSelectorStr, ".", 2)
	if len(conTypeSel) != 2 {
		return fmt.Errorf("not a valid connection selector : %s", c.ConnSelectorStr)
	}
	c.Type = strings.ToLower(conTypeSel[0])
	c.CfgKey = strings.ToLower(conTypeSel[1])
	return nil
}

func (c *ConSelector) ReadCfgFromYaml() (props map[string]interface{}, err error) {

	var (
		found = false
	)

	cfg := make(map[string]interface{})
	err = LoadConfigByName(CONNECTION_CONF, &cfg)
	if err != nil {
		return nil, err
	}

	if cons, ok := cfg[c.Type]; ok {
		if connItems, ok1 := cons.(map[string]interface{}); ok1 {
			if conItem, ok := connItems[c.CfgKey]; ok {
				if item, ok1 := conItem.(map[string]interface{}); ok1 {
					props = item
					found = true
				}
			}
		}
	}
	if !found {
		return nil, fmt.Errorf("not found connection Type and Selector:  %s.%s", c.Type, c.CfgKey)
	}

	jsonPath := "sources/" + c.Type + ".json"
	if c.Type == "mqtt" {
		jsonPath = "mqtt_source.json"
	}

	err = CorrectsConfigKeysByJson(props, jsonPath)
	return props, err
}
