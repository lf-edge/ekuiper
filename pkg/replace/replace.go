package replace

import "encoding/json"

var (
	replaceURL      = []string{"url"}
	replacePassword = []string{"saslPassword"}
	replaceAction   = map[string]struct{}{
		"kafka": {},
		"sql":   {},
	}
)

func ReplaceRuleJson(ruleJson string) string {
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
				actionPropsMap = ReplacePropsDBURL(ReplacePassword(actionPropsMap))
				actionMap[actionTyp] = actionPropsMap
				actions[index] = actionMap
				break
			}
		}
	}
	m["actions"] = actions
	got, err := json.Marshal(m)
	if err != nil {
		return ruleJson
	}
	return string(got)
}

func ReplacePropsDBURL(props map[string]interface{}) map[string]interface{} {
	for _, replaceWord := range replaceURL {
		v, ok := props[replaceWord]
		if ok {
			props["dburl"] = v
			delete(props, replaceWord)
			break
		}
	}
	return props
}

func ReplacePassword(props map[string]interface{}) map[string]interface{} {
	for _, replaceWord := range replacePassword {
		v, ok := props[replaceWord]
		if ok {
			props["password"] = v
			delete(props, replaceWord)
			break
		}
	}
	return props
}
