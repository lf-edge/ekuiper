package xsql

import (
	"fmt"
	"github.com/emqx/kuiper/common"
	"github.com/emqx/kuiper/plugins"
	"github.com/emqx/kuiper/xstream/api"
	"strings"
)

type AggregateFunctionValuer struct {
	data    AggregateData
	fv      *FunctionValuer
	plugins map[string]api.Function
}

//Should only be called by stream to make sure a single instance for an operation
func NewAggregateFunctionValuers() (*FunctionValuer, *AggregateFunctionValuer) {
	fv := &FunctionValuer{}
	return fv, &AggregateFunctionValuer{
		fv: fv,
	}
}

func (v *AggregateFunctionValuer) SetData(data AggregateData) {
	v.data = data
}

func (v *AggregateFunctionValuer) GetSingleCallValuer() CallValuer {
	return v.fv
}

func (v *AggregateFunctionValuer) Value(key string) (interface{}, bool) {
	return nil, false
}

func (v *AggregateFunctionValuer) Meta(key string) (interface{}, bool) {
	return nil, false
}

func (v *AggregateFunctionValuer) Call(name string, args []interface{}) (interface{}, bool) {
	lowerName := strings.ToLower(name)
	switch lowerName {
	case "avg":
		arg0 := args[0].([]interface{})
		if len(arg0) > 0 {
			v := getFirstValidArg(arg0)
			switch v.(type) {
			case int, int64:
				if r, err := sliceIntTotal(arg0); err != nil {
					return err, false
				} else {
					return r / len(arg0), true
				}
			case float64:
				if r, err := sliceFloatTotal(arg0); err != nil {
					return err, false
				} else {
					return r / float64(len(arg0)), true
				}
			default:
				return fmt.Errorf("run avg function error: found invalid arg %[1]T(%[1]v)", v), false
			}
		}
		return 0, true
	case "count":
		arg0 := args[0].([]interface{})
		return len(arg0), true
	case "max":
		arg0 := args[0].([]interface{})
		if len(arg0) > 0 {
			v := getFirstValidArg(arg0)
			switch t := v.(type) {
			case int:
				if r, err := sliceIntMax(arg0, t); err != nil {
					return err, false
				} else {
					return r, true
				}
			case int64:
				if r, err := sliceIntMax(arg0, int(t)); err != nil {
					return err, false
				} else {
					return r, true
				}
			case float64:
				if r, err := sliceFloatMax(arg0, t); err != nil {
					return err, false
				} else {
					return r, true
				}
			case string:
				if r, err := sliceStringMax(arg0, t); err != nil {
					return err, false
				} else {
					return r, true
				}
			default:
				return fmt.Errorf("run max function error: found invalid arg %[1]T(%[1]v)", v), false
			}
		}
		return fmt.Errorf("run max function error: empty data"), false
	case "min":
		arg0 := args[0].([]interface{})
		if len(arg0) > 0 {
			v := getFirstValidArg(arg0)
			switch t := v.(type) {
			case int:
				if r, err := sliceIntMin(arg0, t); err != nil {
					return err, false
				} else {
					return r, true
				}
			case int64:
				if r, err := sliceIntMin(arg0, int(t)); err != nil {
					return err, false
				} else {
					return r, true
				}
			case float64:
				if r, err := sliceFloatMin(arg0, t); err != nil {
					return err, false
				} else {
					return r, true
				}
			case string:
				if r, err := sliceStringMin(arg0, t); err != nil {
					return err, false
				} else {
					return r, true
				}
			default:
				return fmt.Errorf("run min function error: found invalid arg %[1]T(%[1]v)", v), false
			}
		}
		return fmt.Errorf("run min function error: empty data"), false
	case "sum":
		arg0 := args[0].([]interface{})
		if len(arg0) > 0 {
			v := getFirstValidArg(arg0)
			switch v.(type) {
			case int, int64:
				if r, err := sliceIntTotal(arg0); err != nil {
					return err, false
				} else {
					return r, true
				}
			case float64:
				if r, err := sliceFloatTotal(arg0); err != nil {
					return err, false
				} else {
					return r, true
				}
			default:
				return fmt.Errorf("run sum function error: found invalid arg %[1]T(%[1]v)", v), false
			}
		}
		return 0, true
	default:
		common.Log.Debugf("run aggregate func %s", name)
		var (
			nf  api.Function
			ok  bool
			err error
		)
		if nf, ok = v.plugins[name]; !ok {
			nf, err = plugins.GetFunction(name)
			if err != nil {
				return err, false
			}
			v.plugins[name] = nf
		}
		if !nf.IsAggregate() {
			return nil, false
		}
		result, ok := nf.Exec(args)
		common.Log.Debugf("run custom aggregate function %s, get result %v", name, result)
		return result, ok
	}
}

func (v *AggregateFunctionValuer) GetAllTuples() AggregateData {
	return v.data
}

func getFirstValidArg(s []interface{}) interface{} {
	for _, v := range s {
		if v != nil {
			return v
		}
	}
	return nil
}

func sliceIntTotal(s []interface{}) (int, error) {
	var total int
	for _, v := range s {
		if vi, ok := v.(int); ok {
			total += vi
		} else {
			return 0, fmt.Errorf("requires int but found %[1]T(%[1]v)", v)
		}
	}
	return total, nil
}

func sliceFloatTotal(s []interface{}) (float64, error) {
	var total float64
	for _, v := range s {
		if vf, ok := v.(float64); ok {
			total += vf
		} else {
			return 0, fmt.Errorf("requires float64 but found %[1]T(%[1]v)", v)
		}
	}
	return total, nil
}
func sliceIntMax(s []interface{}, max int) (int, error) {
	for _, v := range s {
		if vi, ok := v.(int); ok {
			if max < vi {
				max = vi
			}
		} else {
			return 0, fmt.Errorf("requires int but found %[1]T(%[1]v)", v)
		}
	}
	return max, nil
}
func sliceFloatMax(s []interface{}, max float64) (float64, error) {
	for _, v := range s {
		if vf, ok := v.(float64); ok {
			if max < vf {
				max = vf
			}
		} else {
			return 0, fmt.Errorf("requires float64 but found %[1]T(%[1]v)", v)
		}
	}
	return max, nil
}

func sliceStringMax(s []interface{}, max string) (string, error) {
	for _, v := range s {
		if vs, ok := v.(string); ok {
			if max < vs {
				max = vs
			}
		} else {
			return "", fmt.Errorf("requires string but found %[1]T(%[1]v)", v)
		}
	}
	return max, nil
}
func sliceIntMin(s []interface{}, min int) (int, error) {
	for _, v := range s {
		if vi, ok := v.(int); ok {
			if min > vi {
				min = vi
			}
		} else {
			return 0, fmt.Errorf("requires int but found %[1]T(%[1]v)", v)
		}
	}
	return min, nil
}
func sliceFloatMin(s []interface{}, min float64) (float64, error) {
	for _, v := range s {
		if vf, ok := v.(float64); ok {
			if min > vf {
				min = vf
			}
		} else {
			return 0, fmt.Errorf("requires float64 but found %[1]T(%[1]v)", v)
		}
	}
	return min, nil
}

func sliceStringMin(s []interface{}, min string) (string, error) {
	for _, v := range s {
		if vs, ok := v.(string); ok {
			if min < vs {
				min = vs
			}
		} else {
			return "", fmt.Errorf("requires string but found %[1]T(%[1]v)", v)
		}
	}
	return min, nil
}
