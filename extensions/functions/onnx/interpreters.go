// Copyright 2022-2024 EMQ Technologies Co., Ltd.
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

// go:build tflite

package main

import (
	"errors"
	"path/filepath"
	"sync"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	// "github.com/lf-edge/ekuiper/v2/pkg/cast"

	"fmt"
	_ "image"
	_ "image/color"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	// "os"
	// "os/exec"

	// "github.com/lf-edge/ekuiper/contract/v2/api"
	ort "github.com/yalue/onnxruntime_go"
)

var ipManager *interpreterManager

func init() {
	// path, err := conf.GetDataLoc() /todo
	// if err != nil {
	// 	panic(err)
	// }
	ipManager = &interpreterManager{
		registry: make(map[string]*InterPreter),
		// path:     filepath.Join(path, "uploads"), todo
		path: filepath.Join("etc"),
	}
}

type interpreterManager struct {
	once       sync.Once
	envInitErr error
	sync.Mutex
	registry map[string]*InterPreter
	path     string
}

func (m *interpreterManager) GetOrCreate(name string) (*InterPreter, error) {
	m.once.Do(
		func() {
			log := conf.Log
			// ort.SetSharedLibraryPath("./data/functions/mnist/onnxruntime.so") //todo 修改到共享路径
			ort.SetSharedLibraryPath("./etc/onnxruntime.so") //todo 修改到共享路径
			err := ort.InitializeEnvironment()
			if err != nil {
				m.envInitErr = fmt.Errorf("failed to initialize environment: %s", err)
				log.Error(m.envInitErr.Error())
			}
		})
	if m.envInitErr != nil {
		return nil, m.envInitErr
	}

	log := conf.Log
	m.Lock()
	defer m.Unlock()
	ip, ok := m.registry[name]
	if !ok {
		mf := filepath.Join(m.path, name+".onnx")
		inputsInfo, outputsInfo, err := ort.GetInputOutputInfo(mf)
		if err != nil {
			log.Errorf("error getting input and output info for %s: %s", mf, err)
			return nil, fmt.Errorf("error getting input and output info for %s: %w", mf, err)
		}

		log.Infof("success load model: %s", mf)
		// defer model.Delete()

		inputsNames := func() []string {
			inputsNames := make([]string, len(inputsInfo))
			for i, info := range inputsInfo {
				inputsNames[i] = info.Name
			}
			return inputsNames
		}()
		outputsNames := func() []string {
			outputsNames := make([]string, len(outputsInfo))
			for i, info := range outputsInfo {
				outputsNames[i] = info.Name
			}
			return outputsNames
		}()
		session, err := ort.NewDynamicAdvancedSession(mf,
			inputsNames, outputsNames, nil)
		if err != nil {
			log.Errorf("error creating MNIST network session: %s", err)
			return nil, fmt.Errorf("error creating MNIST network session: %w", err)
		}

		if len(inputsInfo) == 0 || len(outputsInfo) == 0 {
			log.Errorf(" input and output length shoulder bigger than 0 ")
			return nil, fmt.Errorf(" input and output length shoulder bigger than 0 ")
		}
		if inputsInfo[0].DataType != outputsInfo[0].DataType {
			log.Errorf(" input and output dataType should be same ")
			return nil, fmt.Errorf(" input and output dataType should be same ")
		}

		testTensor, err := ort.NewTensor(ort.NewShape(1, 1, 1, 1), make([]float32, 1))
		if err != nil {
			log.Errorf("error creating input tensor: %s", err)
			return nil, fmt.Errorf("error creating input tensor: %w", err)
		}
		log.Infof("success allocate tensors for: %s", mf)

		defer func() {
			log.Infof("inputTensor.Destroy() start2")
			testTensor.Destroy()
			log.Infof("inputTensor.Destroy() success")
		}()

		m.registry[name] = NewInterPreter(session, inputsInfo, outputsInfo)
		ip = m.registry[name]
		log.Infof("inputTensor.Destroy() start1")
	}
	return ip, nil
}

// // GetTensorDataType 从tensorElement数据类型获取golang数据类型
// func GetTensorDataType(tensorElementDataType ort.TensorElementDataType) (any, error) {

// 	switch tensorElementDataType {
// 	//same as github.com/yalue/onnxruntime_go@v1.9.0/onnxruntime_go.go
// 	// todo:some not support ，like TensorElementDataTypeFloat8E4M3FN 、TensorElementDataTypeFloat16
// 	case ort.TensorElementDataTypeFloat:
// 		return float32(1), nil
// 	case ort.TensorElementDataTypeUint8:
// 		return uint8(1), nil
// 	case ort.TensorElementDataTypeInt8:
// 		return int8(1), nil
// 	case ort.TensorElementDataTypeUint16:
// 		return uint16(1), nil
// 	case ort.TensorElementDataTypeInt16:
// 		return int16(1), nil
// 	case ort.TensorElementDataTypeInt32:
// 		return int32(1), nil
// 	case ort.TensorElementDataTypeInt64:
// 		return int64(1), nil
// 	case ort.TensorElementDataTypeDouble:
// 		return float64(1), nil
// 	case ort.TensorElementDataTypeUint32:
// 		return uint32(1), nil
// 	case ort.TensorElementDataTypeUint64:
// 		return uint64(1), nil
// 	}
// 	return 0, errors.New("not support tensorElementDataType")

// }

// NewEmptyTensor
//
//	传入 TensorElementDataType数字类型 和形状
// //	传出，*ort.Tensor[]
// func NewEmptyTensor[T ort.TensorData](tensorElementDataType ort.TensorElementDataType, shape ort.Shape) (*ort.Tensor[T], error) {

// 	switch tensorElementDataType {
// 	// //same as github.com/yalue/onnxruntime_go@v1.9.0/onnxruntime_go.go
// 	// // todo:some not support ，like TensorElementDataTypeFloat8E4M3FN 、TensorElementDataTypeFloat16 看下能否支持
// 	case ort.TensorElementDataTypeFloat:
// 		t, err := ort.NewEmptyTensor[float32](shape)
// 		return t, err
// 		//todo :!!!!为何错误，如何改正
// 	// case ort.TensorElementDataTypeUint8:
// 	// 	fallthrough
// 	// case ort.TensorElementDataTypeInt8:
// 	// 	fallthrough
// 	// case ort.TensorElementDataTypeUint16:
// 	// 	fallthrough
// 	// case ort.TensorElementDataTypeInt16:
// 	// 	fallthrough
// 	// case ort.TensorElementDataTypeInt32:
// 	// 	fallthrough
// 	// case ort.TensorElementDataTypeInt64:
// 	// 	fallthrough
// 	// case ort.TensorElementDataTypeDouble:
// 	// 	fallthrough
// 	case ort.TensorElementDataTypeUint32:
// 		t, err := ort.NewEmptyTensor[uint32](shape)
// 		return t, err
// 	case ort.TensorElementDataTypeUint64:
// 		t, err := ort.NewEmptyTensor[uint64](shape)
// 		return t, err
// 	}
// 	// return nil, errors.New("not support tensorElementDataType")

// }

type InterPreter struct {
	session    *ort.DynamicAdvancedSession
	inputInfo  []ort.InputOutputInfo
	outputInfo []ort.InputOutputInfo
}

func NewInterPreter(session *ort.DynamicAdvancedSession,
	inputInfo []ort.InputOutputInfo,
	outputInfo []ort.InputOutputInfo) *InterPreter {
	return &InterPreter{
		session:    session,
		inputInfo:  inputInfo,
		outputInfo: outputInfo,
	}
}

func (ip *InterPreter) GetInputTensorCount() int {
	return len(ip.inputInfo)
}

func (ip *InterPreter) GetEmptyOutputTensors() ([]ort.ArbitraryTensor, error) {
	if len(ip.outputInfo) == 0 {
		return nil, errors.New("output len should bigger than 0~")
	}

	for i := 1; i < len(ip.outputInfo); i++ {
		if ip.outputInfo[i].DataType != ip.outputInfo[i-1].DataType {
			return nil, errors.New("output tensorElementDataType should be same~")
		}
	}
	var dataType ort.TensorElementDataType = ip.outputInfo[0].DataType
	var emptyOutputTensors []ort.ArbitraryTensor
	for _, outputInfo := range ip.outputInfo {
		emptyOutputTensor, err := newEmptyArbitraryTensorBydataType(dataType, outputInfo.Dimensions)
		if err != nil { //todo 内存泄漏
			return nil, err
		}
		emptyOutputTensors = append(emptyOutputTensors, emptyOutputTensor)
	}
	return emptyOutputTensors, nil

}

func newEmptyArbitraryTensorBydataType(dataType ort.TensorElementDataType, shape ort.Shape,notSupportedDataLen int) (ort.ArbitraryTensor, error) {

	switch dataType {
	case ort.TensorElementDataTypeFloat:
		return ort.NewEmptyTensor[float32](shape)
	case ort.TensorElementDataTypeUint8:
		return ort.NewEmptyTensor[uint8](shape)
	case ort.TensorElementDataTypeInt8:
		return ort.NewEmptyTensor[int8](shape)
	case ort.TensorElementDataTypeUint16:
		return ort.NewEmptyTensor[uint16](shape)
	case ort.TensorElementDataTypeInt16:
		return ort.NewEmptyTensor[int16](shape)
	case ort.TensorElementDataTypeInt32:
		return ort.NewEmptyTensor[int32](shape)
	case ort.TensorElementDataTypeInt64:
		return ort.NewEmptyTensor[int64](shape)
	case ort.TensorElementDataTypeDouble:
		return ort.NewEmptyTensor[float64](shape)
	case ort.TensorElementDataTypeUint32:
		return ort.NewEmptyTensor[uint32](shape)
	case ort.TensorElementDataTypeUint64:
		return ort.NewEmptyTensor[uint64](shape)
	default:
		// totalSize := shape.FlattenedSize() //todo 其他数据类型
		// actualData := unsafe.Slice((*byte)(tensorData), totalSize)
		// return NewCustomDataTensor(shape, actualData, tensorType)
		return nil, errors.New("not support tensorElementDataType")
	}
}

func newEmptyArbitraryTensorBydataTypeFloat32(shape ort.Shape) (*ort.Tensor[float32], error) {
	return ort.NewEmptyTensor[float32](shape)
}
