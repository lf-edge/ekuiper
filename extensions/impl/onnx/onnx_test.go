// Copyright 2022-2025 EMQ Technologies Co., Ltd.
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
	"image"
	"image/color"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"math"
	"os"
	"reflect"
	"testing"

	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

func Test_mnist_Exec(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	type args struct {
		in0  api.FunctionContext
		args []any
	}
	tests := []struct {
		name  string
		args  args
		want  any
		want2 any
		want1 bool
	}{
		{
			name: "test1",
			args: args{
				in0: fctx,
				args: func() []any {
					f, _ := os.Open("./img.png")
					originalPic, _, _ := image.Decode(f)
					bounds := originalPic.Bounds().Canon()
					if (bounds.Min.X != 0) || (bounds.Min.Y != 0) {
						// Should never happen with the standard library.
						t.Errorf("image should be canonically encoded")
					}
					inputImage := &ProcessedImage{
						dx:     float32(bounds.Dx()) / 28.0,
						dy:     float32(bounds.Dy()) / 28.0,
						pic:    originalPic,
						Invert: false,
					}

					inputData := inputImage.GetNetworkInput()
					var anyBits []any
					for _, v := range inputData {
						anyBits = append(anyBits, v)
					}
					args := make([]any, 0, 2)
					args = append(args, "mnist")
					args = append(args, anyBits)
					return args
				}(),
			},
			want:  []interface{}{[]float32{1.3509222, 1.1492438, 2.231948, 0.82689315, -3.473754, 1.2002871, -1.185765, -5.960128, 4.7645416, -2.3451788}},
			want2: []any{[]float32{1.3509218, 1.1492435, 2.2319477, 0.82689494, -3.4737532, 1.2002872, -1.1857648, -5.9601254, 4.7645416, -2.3451786}},
			want1: true,
		},
		{
			name: "test2",
			args: args{
				in0: fctx,
				args: func() []any {
					args := make([]any, 0, 2)
					args = append(args, "sum_and_difference")
					args = append(args, []any{0.2, 0.3, 0.6, 0.9})
					return args
				}(),
			},
			want:  []any{[]float32{1.9999883, 0.60734314}},
			want1: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &OnnxFunc{}

			got0, got1 := f.Exec(tt.args.in0, tt.args.args)
			if got1 != tt.want1 {
				t.Errorf("Exec() error = %v, wantErr %v", got1, tt.want1)
			}
			if !almostEqual(got0, tt.want) && !reflect.DeepEqual(got1, tt.want2) {
				t.Errorf("Name = %s,Exec() got = %v, want %v", tt.name, got0, tt.want)
			}
			if tt.name == "test2" {
				test2Ouput := got0.([]any)[0].([]float32)
				if math.Abs(float64(test2Ouput[0]-1.9999883)) > 1e-4 {
					t.Errorf("Name = %s, Exec() got = %v, want %v", tt.name, test2Ouput[0], 1.9999883)
				}
			}
		})
	}
}

func almostEqual(got, want any) bool {
	if reflect.DeepEqual(got, want) {
		return true
	}
	vGot := reflect.ValueOf(got)
	vWant := reflect.ValueOf(want)
	if vGot.Kind() != vWant.Kind() {
		return false
	}
	switch vGot.Kind() {
	case reflect.Slice, reflect.Array:
		if vGot.Len() != vWant.Len() {
			return false
		}
		for i := 0; i < vGot.Len(); i++ {
			if !almostEqual(vGot.Index(i).Interface(), vWant.Index(i).Interface()) {
				return false
			}
		}
		return true
	case reflect.Float32, reflect.Float64:
		const epsilon = 1e-4
		return math.Abs(vGot.Float()-vWant.Float()) <= epsilon
	default:
		return false
	}
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

// 辅助图片类
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
