// Copyright 2025 EMQ Technologies Co., Ltd.
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

package replace

import (
	"encoding/json"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
)

// ReplaceCold This is a hack for yupik L946.
func ReplaceCold(rule map[string]any) (string, bool) {
	idStr := ""
	id, ok := rule["id"]
	if ok {
		idStr, ok = id.(string)
	}
	if conf.Config.Hack.Cold && ok && idStr == "cold" {
		conf.Log.Info("replacing cold rule")
		actions, exist := rule["actions"].([]any)
		if !exist {
			conf.Log.Info("replacing cold rule exit due to no actions defined")
			return "", false
		}
		changed := false
		for index, action := range actions {
			actionMap, ok := action.(map[string]interface{})
			if !ok {
				continue
			}
			for actionTyp, actionProps := range actionMap {
				if actionTyp == "rest" {
					actionPropsMap, ok2 := actionProps.(map[string]any)
					if ok2 {
						formdata, ok3 := actionPropsMap["formData"].(map[string]any)
						if ok3 {
							newAction := make(map[string]any)
							formdata["url"] = actionPropsMap["url"]
							newAction["server"] = "tcp://127.0.0.1:1883"
							newAction["topic"] = "network_bridge/post"
							newAction["protocolVersion"] = "5"
							newAction["properties"] = formdata
							newAction["format"] = actionPropsMap["format"]
							newAction["sendSingle"] = actionPropsMap["sendSingle"]
							newAction["lingerInterval"] = actionPropsMap["lingerInterval"]
							newAction["compression"] = actionPropsMap["compression"]
							newAction["compressionProps"] = actionPropsMap["compressionProps"]
							newAction["encryption"] = actionPropsMap["encryption"]
							newAction["encProps"] = actionPropsMap["encProps"]
							actionMap["mqtt"] = newAction
							delete(actionMap, "rest")
							changed = true
						} else {
							conf.Log.Info("replacing cold rule exit due to no formData defined")
						}
					}
					// each action only have 1 type
					break
				}
			}
			if changed {
				actions[index] = actionMap
			}
		}
		if changed {
			newRuleJson, _ := json.Marshal(rule)
			conf.Log.Info("replaced cold rule")
			return string(newRuleJson), true
		}
	}
	return "", false
}
