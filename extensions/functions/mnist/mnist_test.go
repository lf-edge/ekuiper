package main

import (
	"fmt"
	"github.com/lf-edge/ekuiper/contract/v2/api"
	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
	ort "github.com/yalue/onnxruntime_go"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"reflect"
	"runtime"
	"sync"
	"testing"
)

func Test_mnist_Exec(t *testing.T) {
	contextLogger := conf.Log.WithField("rule", "testExec")
	ctx := kctx.WithValue(kctx.Background(), kctx.LoggerKey, contextLogger)
	tempStore, _ := state.CreateStore("mockRule0", def.AtMostOnce)
	fctx := kctx.NewDefaultFuncContext(ctx.WithMeta("mockRule0", "test", tempStore), 2)

	type fields struct {
		modelPath         string
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
		want0  any
		want1  bool
	}{
		{
			name: "test1",
			fields: fields{
				modelPath:         "etc/mnist.onnx",
				inputShape:        ort.NewShape(1, 1, 28, 28),
				outputShape:       ort.NewShape(1, 10),
				sharedLibraryPath: getDefaultSharedLibPath(),
				initModelError:    nil,
			},
			args: args{
				in0: fctx,
				args: func() []any {
					args := make([]any, 0)
					bits, _ := os.ReadFile("./img.png")
					args = append(args, bits)
					return args
				}(),
			},
			want0: "Output probabilities:\n" +
				fmt.Sprintf("  %d: %f\n", 0, 1.350922) +
				fmt.Sprintf("  %d: %f\n", 1, 1.1492438) +
				fmt.Sprintf("  %d: %f\n", 2, 2.231948) +
				fmt.Sprintf("  %d: %f\n", 3, 0.82689315) +
				fmt.Sprintf("  %d: %f\n", 4, -3.473754) +
				fmt.Sprintf("  %d: %f\n", 5, 1.2002871) +
				fmt.Sprintf("  %d: %f\n", 6, -1.185765) +
				fmt.Sprintf("  %d: %f\n", 7, -5.960128) +
				fmt.Sprintf("  %d: %f\n", 8, 4.7645416) +
				fmt.Sprintf("  %d: %f\n", 9, -2.3451788) +
				fmt.Sprintf(" probably a %d, with probability %f\n", 8, 4.764542),
			want1: true,
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

			got0, got1 := f.Exec(tt.args.in0, tt.args.args)
			if !reflect.DeepEqual(got0, tt.want0) {
				t.Errorf("Exec() got0 = %v, want0 = %v", got0, tt.want0)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Exec() got1 = %v, want1 %v", got1, tt.want1)
			}
		})
	}
}

func getDefaultSharedLibPath() string {
	// For now, we only include libraries for x86_64 windows, ARM64 darwin, and
	// x86_64 or ARM64 Linux. In the future, libraries may be added or removed.
	if runtime.GOOS == "windows" {
		if runtime.GOARCH == "amd64" {
			return "lib/onnxruntime.dll"
		}
	}
	if runtime.GOOS == "darwin" {
		if runtime.GOARCH == "arm64" {
			return "lib/onnxruntime_arm64.dylib"
		}
	}
	if runtime.GOOS == "linux" {
		if runtime.GOARCH == "arm64" {
			return "lib/onnxruntime_arm64.so"
		}
		return "lib/onnxruntime.so"
	}
	fmt.Printf("Unable to determine a path to the onnxruntime shared library"+
		" for OS \"%s\" and architecture \"%s\".\n", runtime.GOOS,
		runtime.GOARCH)
	return ""
}
