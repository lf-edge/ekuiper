// Copyright 2022 EMQ Technologies Co., Ltd.
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

package tensorFlowGeneral

import (
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/lf-edge/ekuiper/pkg/cast"
	"github.com/mattn/go-tflite"
	"strconv"
)

type Tffunc struct {
}

// Validate the arguments.
// args[0]: string, model name which maps to a path
// args[1 to n]: tensors
func (f *Tffunc) Validate(args []interface{}) error {
	if len(args) < 2 {
		return fmt.Errorf("tensorflow function must have at least 2 parameters but got %d", len(args))
	}
	return nil
}

func (f *Tffunc) IsAggregate() bool {
	return false
}

func (f *Tffunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	model, ok := args[0].(string)
	if !ok {
		return fmt.Errorf("tensorflow function first parameter must be a string, but got %[1]T(%[1]v)", args[0]), false
	}
	interpreter, err := ipManager.GetOrCreate(model)
	if err != nil {
		return err, false
	}
	inputCount := interpreter.GetInputTensorCount()
	if len(args)-1 != inputCount {
		return fmt.Errorf("tensorflow function requires %d tensors but got %d", inputCount, len(args)-1), false
	}

	ctx.GetLogger().Warnf("tensorflow function %s with %d tensors", model, inputCount)
	// Set input tensors
	for i := 1; i < len(args); i++ {
		input := interpreter.GetInputTensor(i - 1)
		dims := "("
		for j := 1; j < input.NumDims(); j++ {
			dims += strconv.Itoa(input.Dim(j)) + ","
		}
		dims += ")"
		ctx.GetLogger().Warnf("tensorflow function %s input %d shape %s", model, i, dims)
		var arg []interface{}
		switch v := args[i].(type) {
		case []byte:
			if int(input.ByteSize()) != len(v) {
				return fmt.Errorf("tensorflow function input tensor %d has %d bytes but got %d", i-1, input.ByteSize(), len(v)), false
			}
			input.CopyFromBuffer(v)
			continue
		case []interface{}:
			arg = v
		default:
			return fmt.Errorf("tensorflow function parameter %d must be a bytea or array of bytea, but got %[1]T(%[1]v)", i), false
		}
		t := input.Type()
		ctx.GetLogger().Warnf("tensor %d input dims %d type %s", i-1, input.NumDims(), t)
		for j := 0; j < input.NumDims(); j++ {
			ctx.GetLogger().Warnf("tensor %d input dim %d %d", i-1, j, input.Dim(j))
		}
		switch input.NumDims() {
		case 0, 1:
			return fmt.Errorf("tensorflow function input tensor %d must have at least 2 dimensions but got 1", i-1), false
		case 2:
			if input.Dim(1) != len(arg) {
				return fmt.Errorf("tensorflow function input tensor %d must have %d elements but got %d", i-1, input.Dim(1), len(arg)), false
			}
			switch t {
			case tflite.Float32:
				v, err := cast.ToFloat32Slice(arg, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect float32 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetFloat32s(v)
				if err != nil {
					return nil, false
				}
			case tflite.Int64:
				v, err := cast.ToInt64Slice(arg, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect int64 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetInt64s(v)
				if err != nil {
					return nil, false
				}
			case tflite.Int32:
				v, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
					return cast.ToInt32(input, sn)
				}, "int32", cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect int32 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetInt32s(v.([]int32))
				if err != nil {
					return nil, false
				}
			case tflite.Int16:
				v, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
					return cast.ToInt16(input, sn)
				}, "int16", cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect int16 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetInt16s(v.([]int16))
				if err != nil {
					return nil, false
				}
			case tflite.Int8:
				v, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
					return cast.ToInt8(input, sn)
				}, "int8", cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect int8 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetInt8s(v.([]int8))
				if err != nil {
					return nil, false
				}
			case tflite.UInt8:
				v, err := cast.ToBytes(args, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect uint8 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetUint8s(v)
				if err != nil {
					return nil, false
				}
			default:
				return fmt.Errorf("invalid %d parameter, unsupported type %v in the model", i, t), false
			}
		default:
			// TODO support multiple dimensions. Here assume user passes a 1D array.
			switch t {
			case tflite.Float32:
				v, err := cast.ToFloat32Slice(args[i], cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect float32 but got %[2]T(%[2]v)", i, args[i]), false
				}
				err = input.SetFloat32s(v)
				if err != nil {
					return nil, false
				}
			case tflite.Int64:
				v, err := cast.ToInt64Slice(arg, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect int64 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetInt64s(v)
				if err != nil {
					return nil, false
				}
			case tflite.Int32:
				v, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
					return cast.ToInt32(input, sn)
				}, "int32", cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect int32 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetInt32s(v.([]int32))
				if err != nil {
					return nil, false
				}
			case tflite.Int16:
				v, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
					return cast.ToInt16(input, sn)
				}, "int16", cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect int16 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetInt16s(v.([]int16))
				if err != nil {
					return nil, false
				}
			case tflite.Int8:
				v, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
					return cast.ToInt8(input, sn)
				}, "int8", cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect int8 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetInt8s(v.([]int8))
				if err != nil {
					return nil, false
				}
			case tflite.UInt8:
				v, err := cast.ToBytes(args, cast.CONVERT_SAMEKIND)
				if err != nil {
					return fmt.Errorf("invalid %d parameter, expect uint8 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
				}
				err = input.SetUint8s(v)
				if err != nil {
					return nil, false
				}
			default:
				return fmt.Errorf("invalid %d parameter, unsupported type %v in the model", i, t), false
			}
		}
	}
	status := interpreter.Invoke()
	if status != tflite.OK {
		return fmt.Errorf("invoke failed"), false
	}
	outputCount := interpreter.GetOutputTensorCount()
	results := make([]interface{}, outputCount)
	for i := 0; i < outputCount; i++ {
		output := interpreter.GetOutputTensor(i)
		//outputSize := output.Dim(output.NumDims() - 1)
		//b := make([]byte, outputSize)
		//status = output.CopyToBuffer(&b[0])
		//if status != tflite.OK {
		//	return fmt.Errorf("output failed"), false
		//}
		//results[i] = b
		t := output.Type()
		switch t {
		case tflite.Float32:
			results[i] = output.Float32s()
		case tflite.Int64:
			results[i] = output.Int64s()
		case tflite.Int32:
			results[i] = output.Int32s()
		case tflite.Int16:
			results[i] = output.Int16s()
		case tflite.Int8:
			results[i] = output.Int8s()
		case tflite.UInt8:
			results[i] = output.UInt8s()
		default:
			return fmt.Errorf("invalid %d parameter, unsupported type %v in the model", i, t), false
		}
	}
	return results, true
}

var Tflite Tffunc
