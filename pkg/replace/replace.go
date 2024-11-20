package replace

import (
	"encoding/json"
	"time"
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
	changed := false
	m := make(map[string]interface{})
	if err := json.Unmarshal([]byte(ruleJson), &m); err != nil {
		return ruleJson
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
		v, ok := props[replaceWord]
		if ok {
			intRaw, ok := v.(int)
			if ok {
				props[replaceWord] = (time.Duration(intRaw) * time.Millisecond).String()
				changed = true
				continue
			}
			int64Raw, ok := v.(int64)
			if ok {
				props[replaceWord] = (time.Duration(int64Raw) * time.Millisecond).String()
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
