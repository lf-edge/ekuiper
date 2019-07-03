package xsql

import (
	"math"
	"strings"
)

type FunctionValuer struct{}

var _ CallValuer = FunctionValuer{}

func (FunctionValuer) Value(key string) (interface{}, bool) {
	return nil, false
}

func (FunctionValuer) Call(name string, args []interface{}) (interface{}, bool) {
	lowerName := strings.ToLower(name)
	switch lowerName {
	case "round":
		arg0 := args[0].(float64)
		return math.Round(arg0), true
	case "abs":
		arg0 := args[0].(float64)
		return math.Abs(arg0), true
	case "pow":
		arg0, arg1 := args[0].(float64), args[1].(int64)
		return math.Pow(arg0, float64(arg1)), true
	default:
		return nil, false
	}
}
