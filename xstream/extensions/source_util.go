package extensions

import (
	"fmt"
	"strconv"
)

func CastToString(v interface{}) (result string, ok bool) {
	switch v := v.(type) {
	case int:
		return strconv.Itoa(v), true
	case string:
		return v, true
	case bool:
		return strconv.FormatBool(v), true
	case float64, float32:
		return fmt.Sprintf("%.2f", v), true
	default:
		return "", false
	}
}
