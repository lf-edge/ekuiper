package main

import (
	"fmt"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	ort "github.com/yalue/onnxruntime_go"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"sync"
	"testing"
)

func Test_mnist_Exec(t *testing.T) {
	type fields struct {
		modelPath         string
		once              sync.Once
		inputShape        ort.Shape
		outputShape       ort.Shape
		sharedLibraryPath string
		initModelError    error
	}
	type args struct {
		in0  api.FunctionContext
		args []any
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   any
		want1  bool
	}{
		{
			name: "test1",
			fields: fields{
				modelPath:         "etc/mnist.onnx",
				once:              sync.Once{},
				inputShape:        ort.NewShape(1, 1, 28, 28),
				outputShape:       ort.NewShape(1, 10),
				sharedLibraryPath: "etc/onnxruntime.so",
				initModelError:    nil,
			},
			args: args{
				in0: nil,
				args: func() []any {
					args := make([]any, 0)
					bits, _ := os.ReadFile("./img.png")
					args = append(args, bits)
					return args
				}(),
			},
			want:  nil,
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := &MnistFunc{
				modelPath:         tt.fields.modelPath,
				once:              sync.Once{},
				inputShape:        tt.fields.inputShape,
				outputShape:       tt.fields.outputShape,
				sharedLibraryPath: tt.fields.sharedLibraryPath,
				initModelError:    tt.fields.initModelError,
			}

			out, got1 := f.Exec(tt.args.in0, tt.args.args)

			if !got1 {
				t.Errorf("Exec() error = %v, wantErr %v", got1, tt.want1)
			}
			fmt.Println(out)
		})
	}
}
