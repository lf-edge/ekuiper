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
	"time"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

var (
	replaceURL      = []string{"url"}
	replacePassword = []string{"saslPassword"}
	replaceDuration = []string{"cacheTtl", "timeout", "expiration", "interval"}
	replaceAction   = map[string]struct{}{
		"kafka": {},
		"sql":   {},
	}
)

func ReplaceRuleJson(ruleJson string, isTesting bool) string {
	if isTesting {
		return ruleJson
	}
	m := make(map[string]any)
	if err := json.Unmarshal([]byte(ruleJson), &m); err != nil {
		return ruleJson
	}
	newJson, changed := ReplaceCold(m)
	if changed {
		return newJson
	}
	actions, ok := m["actions"].([]interface{})
	if !ok {
		return ruleJson
	}
	for index, action := range actions {
		actionMap, ok := action.(map[string]interface{})
		if !ok {
			continue
		}
		for actionTyp, actionProps := range actionMap {
			_, ok1 := replaceAction[actionTyp]
			actionPropsMap, ok2 := actionProps.(map[string]interface{})
			if ok1 && ok2 {
				replaced, newProps := ReplacePropsWithPlug(actionTyp, actionPropsMap)
				if replaced {
					changed = true
					actionPropsMap = newProps
					actionMap[actionTyp] = actionPropsMap
					actions[index] = actionMap
				}
			}
			// each action only have 1 type
			break
		}
	}
	if !changed {
		return ruleJson
	}
	m["actions"] = actions
	got, err := json.Marshal(m)
	if err != nil {
		return ruleJson
	}
	return string(got)
}

func WithDisableReplaceDburl() ReplacePropsOption {
	return func(c *ReplacePropsConfig) {
		c.DisableReplaceDbUrl = true
	}
}

func WithDisableReplacePassword() ReplacePropsOption {
	return func(c *ReplacePropsConfig) {
		c.DisableReplacePassword = true
	}
}

func ReplacePropsDBURL(props map[string]interface{}) (bool, map[string]interface{}) {
	changed := false
	for _, replaceWord := range replaceURL {
		v, ok := props[replaceWord]
		if ok {
			props["dburl"] = v
			delete(props, replaceWord)
			changed = true
			break
		}
	}
	return changed, props
}

func ReplacePassword(props map[string]interface{}) (bool, map[string]interface{}) {
	changed := false
	for _, replaceWord := range replacePassword {
		v, ok := props[replaceWord]
		if ok {
			props["password"] = v
			delete(props, replaceWord)
			changed = true
			break
		}
	}
	return changed, props
}

func ReplaceDuration(props map[string]interface{}) (bool, map[string]interface{}) {
	changed := false
	for _, replaceWord := range replaceDuration {
		if replaceWord == "cacheTtl" {
			vm, ok := props["lookup"]
			if ok {
				lookupm, ok := vm.(map[string]interface{})
				if ok {
					oldValue, ok := lookupm[replaceWord]
					if ok {
						intRaw, err := cast.ToInt(oldValue, cast.CONVERT_ALL)
						if err == nil {
							lookupm[replaceWord] = (time.Duration(intRaw) * time.Millisecond).String()
							changed = true
							props["lookup"] = lookupm
							continue
						}
					}

				}
			}
		}
		v, ok := props[replaceWord]
		if ok {
			intRaw, err := cast.ToInt(v, cast.CONVERT_ALL)
			if err == nil {
				props[replaceWord] = (time.Duration(intRaw) * time.Millisecond).String()
				changed = true
				continue
			}
		}
	}
	return changed, props
}

func ReplacePropsWithPlug(plug string, props map[string]interface{}) (bool, map[string]interface{}) {
	switch plug {
	case "sql":
		return ReplacePropsWithOption(props)
	default:
		return ReplacePropsWithOption(props, WithDisableReplaceDburl())
	}
}

func ReplacePropsWithOption(props map[string]interface{}, opts ...ReplacePropsOption) (bool, map[string]interface{}) {
	ReplaceConfig := &ReplacePropsConfig{}
	for _, opt := range opts {
		opt(ReplaceConfig)
	}
	replaced := false
	var changed bool
	if !ReplaceConfig.DisableReplacePassword {
		changed, props = ReplacePassword(props)
		replaced = replaced || changed
	}
	if !ReplaceConfig.DisableReplaceDbUrl {
		changed, props = ReplacePropsDBURL(props)
		replaced = replaced || changed
	}
	if !ReplaceConfig.DisableReplaceDuration {
		changed, props = ReplaceDuration(props)
		replaced = replaced || changed
	}
	return replaced, props
}

type ReplacePropsOption func(c *ReplacePropsConfig)

type ReplacePropsConfig struct {
	DisableReplaceDbUrl    bool
	DisableReplacePassword bool
	DisableReplaceDuration bool
}
