package function

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/lf-edge/ekuiper/v2/internal/v2/api"
)

type (
	funcExe    func(ctx context.Context, args []*api.Datum) (*api.Datum, error)
	calRetType func(ctx context.Context, args []*api.Datum) (api.DatumType, error)
)

type builtinFunc struct {
	calRetType
	exec map[api.DatumType]funcExe
}

var (
	builtinFuncs = map[string]builtinFunc{}
)

func init() {
	registerFunc()
}

func CallFunction(ctx context.Context, funcName string, args []*api.Datum) (*api.Datum, error) {
	f, ok := builtinFuncs[funcName]
	if !ok {
		return nil, fmt.Errorf("unknown function: %s", funcName)
	}
	retType, err := f.calRetType(ctx, args)
	if err != nil {
		return nil, err
	}
	callFunc, ok := f.exec[retType]
	if !ok {
		return nil, fmt.Errorf("unsupported function: %s retType:%v", funcName, retType)
	}
	return callFunc(ctx, args)
}

func registerFunc() {
	builtinFuncs["add"] = builtinFunc{
		calRetType: defaultCalculateRetType,
		exec: map[api.DatumType]funcExe{
			api.I64Val: func(ctx context.Context, args []*api.Datum) (*api.Datum, error) {
				v1, err := args[0].GetI64Val()
				if err != nil {
					return nil, err
				}
				v2, err := args[1].GetI64Val()
				if err != nil {
					return nil, err
				}
				return api.NewI64Datum(v1 + v2), nil
			},
			api.F64Val: func(ctx context.Context, args []*api.Datum) (*api.Datum, error) {
				v1, err := args[0].ToF64Val()
				if err != nil {
					return nil, err
				}
				v2, err := args[1].ToF64Val()
				if err != nil {
					return nil, err
				}
				return api.NewF64Datum(v1 + v2), nil
			},
			api.DurationVal: func(ctx context.Context, args []*api.Datum) (*api.Datum, error) {
				d1, err := args[0].DurVal()
				if err != nil {
					return nil, err
				}
				d2, err := args[1].DurVal()
				if err != nil {
					return nil, err
				}
				return api.NewDurDatum(d1 + d2), nil
			},
		},
	}
	builtinFuncs["to_json"] = builtinFunc{
		calRetType: func(ctx context.Context, args []*api.Datum) (api.DatumType, error) {
			return api.StringVal, nil
		},
		exec: map[api.DatumType]funcExe{
			api.StringVal: func(ctx context.Context, args []*api.Datum) (*api.Datum, error) {
				d := args[0]
				switch d.Kind {
				case api.MapVal, api.SliceVal:
					v := d.ToInterface()
					if v == nil {
						return nil, nil
					}
					sv, err := json.Marshal(v)
					if err != nil {
						return nil, err
					}
					return api.NewStringDatum(string(sv)), nil
				default:
					return nil, fmt.Errorf("unsupported type: %v", args[0].Kind)
				}
			},
		},
	}
}

func defaultCalculateRetType(ctx context.Context, args []*api.Datum) (api.DatumType, error) {
	if len(args) == 0 {
		return api.UnknownVal, nil
	}
	commonType := args[0].Kind
	allSame := true
	allNumber := true
	for _, arg := range args {
		if arg.Kind != commonType {
			allSame = false
		}
		if arg.Kind != api.I64Val && arg.Kind != api.F64Val {
			allNumber = false
		}
	}
	if allSame {
		return commonType, nil
	}
	if allNumber {
		return api.F64Val, nil
	}
	return api.UnknownVal, nil
}
