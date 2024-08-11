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
	"image/color"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"os/exec"
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
		return fmt.Errorf("Bounding rect  doesn't start at 0, 0"), false
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
	// session, e := ort.NewAdvancedSession(f.modelPath,
	// 	[]string{"Input3"}, []string{"Plus214_Output_0"},
	// 	[]ort.ArbitraryTensor{input}, []ort.ArbitraryTensor{output}, nil)

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

	var returnRes = "Output probabilities:\n"
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
	sharedLibraryPath: "./data/functions/mnist/onnxruntime.so",
	inputShape:        ort.NewShape(1, 1, 28, 28),
	outputShape:       ort.NewShape(1, 10),
}
var _ api.Function = &MnistFunc{}

func printCurrDIr() string {
	// 创建一个 bytes.Buffer 来捕获命令输出
	var out bytes.Buffer

	// 创建并配置 exec.Command 用于运行 tree 命令
	cmd := exec.Command("tree")

	// 设置命令的标准输出为 bytes.Buffer
	cmd.Stdout = &out

	// 运行命令并检查错误
	err := cmd.Run()
	if err != nil {
		return fmt.Sprintf("Error executing command:%v", err)

	}

	// 将命令输出转换为字符串
	res := out.String()

	// 打印结果
	fmt.Println(res)
	return res
}

func checkFileStat(filePath string) {
	// 确认文件路径

	fmt.Println("checkFileStat File path:", filePath)

	// 检查文件是否存在
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		fmt.Println("File does not exist:", filePath)
	} else {
		fmt.Println("File exists:", filePath)
	}
}

/// 辅助图片类

// Implements the color interface
type grayscaleFloat float32

func (f grayscaleFloat) RGBA() (r, g, b, a uint32) {
	a = 0xffff
	v := uint32(f * 0xffff)
	if v > 0xffff {
		v = 0xffff
	}
	r = v
	g = v
	b = v
	return
}

// ProcessedImage Used to satisfy the image interface as well as to help with formatting and
// resizing an input image into the format expected as a network input.
type ProcessedImage struct {
	// The number of "pixels" in the input image corresponding to a single
	// pixel in the 28x28 output image.
	dx, dy float32

	// The input image being transformed
	pic image.Image

	// If true, the grayscale values in the postprocessed image will be
	// inverted, so that dark colors in the original become light, and vice
	// versa. Recall that the network expects black backgrounds, so this should
	// be set to true for images with light backgrounds.
	Invert bool
}

func (p *ProcessedImage) ColorModel() color.Model {
	return color.Gray16Model
}

func (p *ProcessedImage) Bounds() image.Rectangle {
	return image.Rect(0, 0, 28, 28)
}

// At Returns an average grayscale value using the pixels in the input image.
func (p *ProcessedImage) At(x, y int) color.Color {
	if (x < 0) || (x >= 28) || (y < 0) || (y >= 28) {
		return color.Black
	}

	// Compute the window of pixels in the input image we'll be averaging.
	startX := int(float32(x) * p.dx)
	endX := int(float32(x+1) * p.dx)
	if endX == startX {
		endX = startX + 1
	}
	startY := int(float32(y) * p.dy)
	endY := int(float32(y+1) * p.dy)
	if endY == startY {
		endY = startY + 1
	}

	// Compute the average brightness over the window of pixels
	var sum float32
	var nPix int
	for row := startY; row < endY; row++ {
		for col := startX; col < endX; col++ {
			c := p.pic.At(col, row)
			grayValue := color.Gray16Model.Convert(c).(color.Gray16).Y
			sum += float32(grayValue) / 0xffff
			nPix++
		}
	}

	brightness := grayscaleFloat(sum / float32(nPix))
	if p.Invert {
		brightness = 1.0 - brightness
	}
	return brightness
}

// GetNetworkInput Returns a slice of data that can be used as the input to the onnx network.
func (p *ProcessedImage) GetNetworkInput() []float32 {
	toReturn := make([]float32, 0, 28*28)
	for row := 0; row < 28; row++ {
		for col := 0; col < 28; col++ {
			c := float32(p.At(col, row).(grayscaleFloat))
			toReturn = append(toReturn, c)
		}
	}
	return toReturn
}
