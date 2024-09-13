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

package main

import (
	"bytes"
	"fmt"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	ort "github.com/yalue/onnxruntime_go"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"sync"
)

type MnistFunc struct {
	modelPath         string
	once              sync.Once
	inputShape        ort.Shape
	outputShape       ort.Shape
	sharedLibraryPath string
	initModelError    error
}

func (f *MnistFunc) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("labelImage function only supports 1 parameter but got %d", len(args))
	}
	return nil
}

func (f *MnistFunc) Exec(_ api.FunctionContext, args []any) (any, bool) {
	arg0, ok := args[0].([]byte)
	if !ok {
		return fmt.Errorf("labelImage function parameter must be a bytea, but got %[1]T(%[1]v)", args[0]), false
	}
	originalPic, _, err := image.Decode(bytes.NewReader(arg0))
	if err != nil {
		return err, false
	}

	f.once.Do(
		func() {
			ort.SetSharedLibraryPath(f.sharedLibraryPath)
			err := ort.InitializeEnvironment()
			if err != nil {
				f.initModelError = fmt.Errorf("failed to initialize environment: %s", err)
				return
			}

			_, _, err = ort.GetInputOutputInfo(f.modelPath)
			if err != nil {
				f.initModelError = fmt.Errorf("error getting input and output info for %s: %w", f.modelPath, err)
				return
			}
		})

	if f.initModelError != nil {
		return fmt.Errorf("%v", f.initModelError), false
	}

	bounds := originalPic.Bounds().Canon()
	if (bounds.Min.X != 0) || (bounds.Min.Y != 0) {
		// Should never happen with the standard library.
		return fmt.Errorf("bounding rect doesn't start at 0, 0"), false
	}
	inputImage := &ProcessedImage{
		dx:     float32(bounds.Dx()) / 28.0,
		dy:     float32(bounds.Dy()) / 28.0,
		pic:    originalPic,
		Invert: false,
	}

	inputData := inputImage.GetNetworkInput()
	input, e := ort.NewTensor(f.inputShape, inputData)
	if e != nil {
		return fmt.Errorf("error creating input tensor: %w", e), false
	}
	defer input.Destroy()

	// Create the output tensor
	output, e := ort.NewEmptyTensor[float32](f.outputShape)
	if e != nil {
		return fmt.Errorf("error creating output tensor: %w", e), false
	}
	defer output.Destroy()

	// The input and output names are required by this network; they can be
	// found on the MNIST ONNX models page linked in the README.

	session, e := ort.NewDynamicAdvancedSession(f.modelPath,
		[]string{"Input3"}, []string{"Plus214_Output_0"}, nil)
	if e != nil {
		return fmt.Errorf("error creating MNIST network session: %w", e), false
	}
	defer session.Destroy()

	// Run the network and print the results.
	e = session.Run([]ort.ArbitraryTensor{input}, []ort.ArbitraryTensor{output})
	if e != nil {
		return fmt.Errorf("error running the MNIST network: %w", e), false
	}

	returnRes := "Output probabilities:\n"
	outputData := output.GetData()
	maxIndex := 0
	maxProbability := float32(-1.0e9)
	for i, v := range outputData {
		returnRes += fmt.Sprintf("  %d: %f\n", i, v)
		if v > maxProbability {
			maxProbability = v
			maxIndex = i
		}
	}
	returnRes += fmt.Sprintf(" probably a %d, with probability %f\n", maxIndex, maxProbability)

	return returnRes, true
}

func (f *MnistFunc) IsAggregate() bool {
	return false
}

var Mnist = MnistFunc{
	modelPath:         "./data/functions/mnist/mnist.onnx",
	sharedLibraryPath: "/usr/local/onnx/lib/onnxruntime.so",
	inputShape:        ort.NewShape(1, 1, 28, 28),
	outputShape:       ort.NewShape(1, 10),
}
var _ api.Function = &MnistFunc{}
