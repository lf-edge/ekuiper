package common

import (
	"encoding/json"
	"fmt"
	"strings"
)

const DefaultField = "self"

func MessageDecode(payload []byte, format string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	switch strings.ToLower(format) {
	case "json":
		e := json.Unmarshal(payload, &result)
		return result, e
	case "binary":
		result[DefaultField] = payload
		return result, nil
	}
	return nil, fmt.Errorf("invalid format %s", format)
}
