package templates

import (
	"encoding/json"
)

//Use the name json in func map
func JsonMarshal(v interface{}) (string, error) {
	if a, err := json.Marshal(v); err != nil {
		return "", err
	} else {
		return string(a), nil
	}
}
