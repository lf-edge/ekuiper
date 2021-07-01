package message

import (
	"encoding/json"
	"fmt"
	"strings"
)

const (
	FormatBinary = "binary"
	FormatJson   = "json"

	DefaultField = "self"
	MetaKey      = "__meta"
)

func Decode(payload []byte, format string) (map[string]interface{}, error) {
	result := make(map[string]interface{})
	switch strings.ToLower(format) {
	case FormatJson:
		e := json.Unmarshal(payload, &result)
		return result, e
	case FormatBinary:
		result[DefaultField] = payload
		return result, nil
	}
	return nil, fmt.Errorf("invalid format %s", format)
}
