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
	"bytes"
	"fmt"
	"github.com/lf-edge/ekuiper/pkg/api"
	"github.com/nfnt/resize"
	"image"
	_ "image/jpeg"
	_ "image/png"
)

type ResizeFunc struct {
}

func (f *ResizeFunc) Validate(args []interface{}) error {
	if len(args) != 4 {
		return fmt.Errorf("The resize function supports 4 parameters, but got %d", len(args))
	}
	return nil
}

func (f *ResizeFunc) IsAggregate() bool {
	return false
}

func (f *ResizeFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	arg, ok := args[0].([]byte)
	if !ok {
		return fmt.Errorf("arg[0] is not a bytea, got %v", args[0]), false
	}
	width, ok := args[1].(int)
	if !ok || 0 > width {
		return fmt.Errorf("arg[1] is not a bigint, got %v", args[1]), false
	}
	height, ok := args[2].(int)
	if !ok || 0 > height {
		return fmt.Errorf("arg[2] is not a bigint, got %v", args[2]), false
	}
	channels, ok := args[3].(int)
	if !ok || 0 > channels {
		return fmt.Errorf("arg[3] is not a bigint, got %v", args[3]), false
	}
	ctx.GetLogger().Debugf("resize: %d %d %d", width, height, channels)
	img, _, err := image.Decode(bytes.NewReader(arg))
	if nil != err {
		return fmt.Errorf("image decode error:%v", err), false
	}
	img = resize.Resize(uint(width), uint(height), img, resize.Bilinear)
	bounds := img.Bounds()
	dx, dy := bounds.Dx(), bounds.Dy()
	bb := make([]byte, width*height*channels)
	for y := 0; y < dy; y++ {
		for x := 0; x < dx; x++ {
			col := img.At(x, y)
			r, g, b, _ := col.RGBA()
			bb[(y*dx+x)*3+0] = byte(float64(r) / 255.0)
			bb[(y*dx+x)*3+1] = byte(float64(g) / 255.0)
			bb[(y*dx+x)*3+2] = byte(float64(b) / 255.0)
		}
	}
	return bb, true
}

var ResizeWithChan ResizeFunc
