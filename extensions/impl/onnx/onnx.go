// Copyright 2021-2025 EMQ Technologies Co., Ltd.
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

package onnx

import (
	_ "bytes"
	"encoding/binary"
	"fmt"
	_ "image"
	_ "image/color"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"

	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/x448/float16"
	ort "github.com/yalue/onnxruntime_go"

	"github.com/lf-edge/ekuiper/v2/pkg/cast"
)

type OnnxFunc struct{}

// Validate the arguments.
// args[0]: string, model name which maps to a path
// args[1 to n]: tensors
func (f *OnnxFunc) Validate(args []interface{}) error {
	if len(args) < 2 {
		return fmt.Errorf("onnx function must have at least 2 parameters but got %d", len(args))
	}
	return nil
}

func (f *OnnxFunc) Exec(ctx api.FunctionContext, args []any) (any, bool) {
	ctx.GetLogger().Debugf("onnx args %[1]T(%[1]v)", args)
	modelName, ok := args[0].(string)
	if !ok {
		return fmt.Errorf("onnx function first parameter must be a string, but got %[1]T(%[1]v)", args[0]), false
	}
	interpreter, err := ipManager.GetOrCreate(modelName)
	if err != nil {
		return err, false
	}
	inputCount := len(interpreter.inputInfo)
	if len(args)-1 != inputCount {
		return fmt.Errorf("onnx function requires %d tensors but got %d", inputCount, len(args)-1), false
	}
	ctx.GetLogger().Debugf("onnx function %s with %d tensors", modelName, inputCount)

	var inputTensors []ort.ArbitraryTensor
	// Set input tensors
	for i := 1; i < len(args); i++ {
		inputInfo := interpreter.inputInfo[i-1]
		var arg []interface{}
		switch v := args[i].(type) {
		case []any: // only supports one dimensional arg. Even dim 0 must be an array of 1 element
			arg = v
		default:
			return fmt.Errorf("onnx function parameter %d must be a bytea or array of bytea, but got %[1]T(%[1]v)", v), false
		}

		notSupportedDataLen := -1
		switch inputInfo.DataType {
		case ort.TensorElementDataTypeDouble:
			value, err := cast.ToFloat64Slice(arg, cast.CONVERT_SAMEKIND, cast.IGNORE_NIL)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect float64 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			input, err := ort.NewTensor(inputInfo.Dimensions, value)
			if err != nil {
				return fmt.Errorf("convert to onnx tensor failed with err %v", err), false
			}
			inputTensors = append(inputTensors, input)
		case ort.TensorElementDataTypeFloat: // convert onnx's type float to float32 of golang
			value, err := cast.ToFloat32Slice(arg, cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect float32 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			input, err := ort.NewTensor(inputInfo.Dimensions, value)
			if err != nil {
				return fmt.Errorf("convert to onnx tensor failed with err %v", err), false
			}
			inputTensors = append(inputTensors, input)
		case ort.TensorElementDataTypeFloat16: // not support
			notSupportedDataLen = 2
			value, err := cast.ToTypedSlice(args, func(input any, sn cast.Strictness) (interface{}, error) {
				f32, err := cast.ToFloat32(input, sn)
				if err != nil {
					return nil, err
				}
				return float16.Fromfloat32(f32), nil
			}, "float16", cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect float32 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			valueFF16, _ := value.([]float16.Float16)
			valueF16 := make([]byte, 0, notSupportedDataLen*2)
			for i := 0; i < len(valueFF16); i++ {
				// The float16.Float16 type is just a uint16 underneath; write its
				// bytes to the data slice.
				binary.LittleEndian.PutUint16(valueF16[2*i:],
					uint16(valueFF16[i]))
			}

			input, err := ort.NewCustomDataTensor(inputInfo.Dimensions, valueF16, ort.TensorElementDataTypeFloat16)
			if err != nil {
				return fmt.Errorf("convert to onnx tensor failed with err %v", err), false
			}
			inputTensors = append(inputTensors, input)
		case ort.TensorElementDataTypeInt64:
			valueI64, err := cast.ToInt64Slice(arg, cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect int64 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			input, err := ort.NewTensor(inputInfo.Dimensions, valueI64)
			if err != nil {
				return fmt.Errorf("convert to onnx tensor failed with err %v", err), false
			}
			inputTensors = append(inputTensors, input)
		case ort.TensorElementDataTypeUint64:
			value, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				return cast.ToUint64(input, sn)
			}, "uin64", cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect uint64 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			valueI32, _ := value.([]uint64)
			input, err := ort.NewTensor(inputInfo.Dimensions, valueI32)
			if err != nil {
				return fmt.Errorf("convert to onnx tensor failed with err %v", err), false
			}
			inputTensors = append(inputTensors, input)

		case ort.TensorElementDataTypeInt32:
			value, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				return cast.ToInt32(input, sn)
			}, "int32", cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect int32 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			valueI32, _ := value.([]int32)
			input, err := ort.NewTensor(inputInfo.Dimensions, valueI32)
			if err != nil {
				return fmt.Errorf("convert to onnx tensor failed with err %v", err), false
			}
			inputTensors = append(inputTensors, input)

		case ort.TensorElementDataTypeUint32:
			value, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				return cast.ToInt32(input, sn)
			}, "int32", cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect float64 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			valueUI32, _ := value.([]uint32)
			input, err := ort.NewTensor(inputInfo.Dimensions, valueUI32)
			if err != nil {
				return fmt.Errorf("convert to onnx tensor failed with err %v", err), false
			}
			inputTensors = append(inputTensors, input)
		case ort.TensorElementDataTypeInt16:
			v, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				return cast.ToInt16(input, sn)
			}, "int16", cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect int16 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			valueI16, _ := v.([]int16)
			input, err := ort.NewTensor(inputInfo.Dimensions, valueI16)
			if err != nil {
				return nil, false
			}
			inputTensors = append(inputTensors, input)

		case ort.TensorElementDataTypeUint16:
			v, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				return cast.ToUint16(input, sn)
			}, "int16", cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect uint16 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			valueUI16, _ := v.([]uint16)
			input, err := ort.NewTensor(inputInfo.Dimensions, valueUI16)
			if err != nil {
				return nil, false
			}
			inputTensors = append(inputTensors, input)
		case ort.TensorElementDataTypeInt8:
			v, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				return cast.ToInt8(input, sn)
			}, "int8", cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect int8 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			valueI8, _ := v.([]int8)
			input, err := ort.NewTensor(inputInfo.Dimensions, valueI8)
			if err != nil {
				return nil, false
			}
			inputTensors = append(inputTensors, input)
		case ort.TensorElementDataTypeUint8:
			v, err := cast.ToTypedSlice(args, func(input interface{}, sn cast.Strictness) (interface{}, error) {
				return cast.ToUint8(input, sn)
			}, "uint8", cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect uint8 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			valueUI8, _ := v.([]uint8)
			input, err := ort.NewTensor(inputInfo.Dimensions, valueUI8)
			if err != nil {
				return nil, false
			}
			inputTensors = append(inputTensors, input)
		case ort.TensorElementDataTypeString: // not support ,but dont need transfer becase string can look as []byte
			v, err := cast.ToBytes(args, cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect string but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}
			valueStr := v
			input, err := ort.NewTensor(inputInfo.Dimensions, valueStr)
			if err != nil {
				return nil, false
			}
			inputTensors = append(inputTensors, input)
		case ort.TensorElementDataTypeBool: // not support ，transfer to []byte
			v, err := cast.ToBytes(args, cast.CONVERT_SAMEKIND)
			if err != nil {
				return fmt.Errorf("invalid %d parameter, expect int8 but got %[2]T(%[2]v) with err %v", i, args[i], err), false
			}

			input, err := ort.NewTensor(inputInfo.Dimensions, v)
			if err != nil {
				return nil, false
			}
			inputTensors = append(inputTensors, input)
		default: // support list see ：GetTensorElementDataType() and TensorElementDataType in onnxruntime_go
			return fmt.Errorf("invalid %d parameter, unsupported type %s in the model", i, inputInfo.DataType), false
		}

		modelParaLen := int64(1)
		for j := 0; j < len(inputInfo.Dimensions); j++ {
			modelParaLen *= inputInfo.Dimensions[j]
		}
		ctx.GetLogger().Debugf("receive tensor %v, require %d length", arg, modelParaLen)
		if modelParaLen != inputTensors[i-1].GetShape().FlattenedSize() {
			return fmt.Errorf("onnx function input tensor %d must have %d elements but got %d", i-1, modelParaLen, len(arg)), false
		}
	}
	// todo :optimize: avoid creating output tensor every time

	outputArbitraryTensors, err := interpreter.GetEmptyOutputTensors()
	if err != nil {
		return err, false
	}

	err = interpreter.session.Run(inputTensors, outputArbitraryTensors)
	if err != nil {
		return fmt.Errorf("run failed,err:%w", err), false
	}

	outputCount := len(interpreter.outputInfo)
	results := make([]any, outputCount)
	outputInfo := interpreter.outputInfo[0]
	for i := 0; i < outputCount; i++ { // for output , only transfer go build-in type
		outputArbitraryTensor := outputArbitraryTensors[i]
		switch outputInfo.DataType {
		case ort.TensorElementDataTypeDouble:
			results[i] = outputArbitraryTensor.(*ort.Tensor[float64]).GetData()
		case ort.TensorElementDataTypeFloat:
			results[i] = outputArbitraryTensor.(*ort.Tensor[float32]).GetData()
		case ort.TensorElementDataTypeFloat16:
			results[i] = outputArbitraryTensor.(*ort.CustomDataTensor).GetData()
		case ort.TensorElementDataTypeInt64:
			results[i] = outputArbitraryTensor.(*ort.Tensor[int64]).GetData()
		case ort.TensorElementDataTypeUint64:
			results[i] = outputArbitraryTensor.(*ort.Tensor[uint64]).GetData()
		case ort.TensorElementDataTypeInt32:
			results[i] = outputArbitraryTensor.(*ort.Tensor[int32]).GetData()
		case ort.TensorElementDataTypeUint32:
			results[i] = outputArbitraryTensor.(*ort.Tensor[uint32]).GetData()
		case ort.TensorElementDataTypeInt16:
			results[i] = outputArbitraryTensor.(*ort.Tensor[int16]).GetData()
		case ort.TensorElementDataTypeUint16:
			results[i] = outputArbitraryTensor.(*ort.Tensor[uint16]).GetData()
		case ort.TensorElementDataTypeInt8:
			results[i] = outputArbitraryTensor.(*ort.Tensor[int8]).GetData()
		case ort.TensorElementDataTypeUint8:
			results[i] = outputArbitraryTensor.(*ort.Tensor[uint8]).GetData()
		case ort.TensorElementDataTypeString:
			results[i] = outputArbitraryTensor.(*ort.CustomDataTensor).GetData()
		case ort.TensorElementDataTypeBool:
			results[i] = outputArbitraryTensor.(*ort.CustomDataTensor).GetData()
		default:
			return fmt.Errorf("invalid %d parameter, unsupported type %s in the model", i, outputInfo.DataType), false
		}

	}
	return results, true
}

func (f *OnnxFunc) IsAggregate() bool {
	return false
}
