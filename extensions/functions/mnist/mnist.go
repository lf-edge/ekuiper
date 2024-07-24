// Copyright 2021-2024 EMQ Technologies Co., Ltd.
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

//go:build tflite

package main

import (
	"fmt"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	ort "github.com/yalue/onnxruntime_go"
	"sync"
)

type mnist struct {
	modelPath   string
	once        sync.Once
	inputShape  ort.Shape
	outputShape ort.Shape
}

func (f *mnist) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("labelImage function only supports 1 parameter but got %d", len(args))
	}
	return nil
}

func (f *mnist) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	// This line _may_ be optional; by default the library will try to load
	// "onnxruntime.dll" on Windows, and "onnxruntime.so" on any other system.
	// For stability, it is probably a good idea to always set this explicitly.
	f.once.Do(func() {
		ort.SetSharedLibraryPath("/usr/local/onnx/onnxruntime_arm64.so")

		err := ort.InitializeEnvironment()
	})

	// For a slight performance boost and convenience when re-using existing
	// tensors, this library expects the user to create all input and output
	// tensors prior to creating the session. If this isn't ideal for your use
	// case, see the DynamicAdvancedSession type in the documnentation, which
	// allows input and output tensors to be specified when calling Run()
	// rather than when initializing a session.
	inputData := []float32{0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9}
	inputShape := f.inputShape
	inputTensor, err := ort.NewTensor(inputShape, inputData)
	defer inputTensor.Destroy()
	// This hypothetical network maps a 2x5 input -> 2x3x4 output.
	outputShape := f.outputShape
	outputTensor, err := ort.NewEmptyTensor[float32](outputShape)
	defer outputTensor.Destroy()

	// The input and output names are required by this network; they can be
	// found on the MNIST ONNX models page linked in the README.
	session, e := ort.NewAdvancedSession(f.modelPath,
		[]string{"Input3"}, []string{"Plus214_Output_0"},
		[]ort.ArbitraryTensor{input}, []ort.ArbitraryTensor{output}, nil)
	defer session.Destroy()

	// Calling Run() will run the network, reading the current contents of the
	// input tensors and modifying the contents of the output tensors.
	err = session.Run()

	// Get a slice view of the output tensor's data.
	outputData := outputTensor.GetData()
	return outputData[0], true
}

func (f *mnist) IsAggregate() bool {
	return false
}

var Mnist = mnist{
	modelPath:   "mnist/nist_float16.onnx",
	inputShape:  ort.NewShape(2, 5),
	outputShape: ort.NewShape(2, 3, 4),
}
