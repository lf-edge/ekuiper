package main

import (
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"os"
	"reflect"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/lf-edge/ekuiper/contract/v2/api"

	"github.com/lf-edge/ekuiper/v2/internal/conf"
	"github.com/lf-edge/ekuiper/v2/internal/pkg/def"
	kctx "github.com/lf-edge/ekuiper/v2/internal/topo/context"
	"github.com/lf-edge/ekuiper/v2/internal/topo/state"
)

// todo 测试文件仿照tf lite
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
					args := make([]any, 0)
					args = append(args, "mnist")
					args = append(args, anyBits)
					return args
				}(),
			},
			want:  []interface{}{[]float32{1.3509222, 1.1492438, 2.231948, 0.82689315, -3.473754, 1.2002871, -1.185765, -5.960128, 4.7645416, -2.3451788}},
			want1: true,
		},
		{
			name: "test2",
			args: args{
				in0: fctx,
				args: func() []any {
					args := make([]any, 0)
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
			if !reflect.DeepEqual(got0, tt.want) {
				t.Errorf("Name = %s,Exec() got = %v, want %v",tt.name, got0, tt.want)
			}
			if tt.name == "test2" {
				test2Ouput := got0.([]any)[0].([]float32)
				if test2Ouput[0] != 1.9999883 {
					t.Errorf("Name = %s, Exec() got = %v, want %v", tt.name,test2Ouput[0], 1.9999883)
				}
			}
		})
	}
}

/*
➜  mnist git:(torch_dev_swx) ✗ go test -v -cover
=== RUN   Test_mnist_Exec
=== RUN   Test_mnist_Exec/test1
Output probabilities:
  0: 1.350922
  1: 1.149244
  2: 2.231948
  3: 0.826893
  4: -3.473754
  5: 1.200287
  6: -1.185765
  7: -5.960128
  8: 4.764542
  9: -2.345179
 probably a 8, with probability 4.764542

-----------------------------
true --- PASS: Test_mnist_Exec (0.03s)
    --- PASS: Test_mnist_Exec/test1 (0.03s)
PASS
coverage: 58.7% of statements
ok      github.com/lf-edge/ekuiper/v2/extensions/functions/mnist        0.030s



*/

func TestPic(t *testing.T) {
	const TOPIC = "tfdmnist"

	images := []string{
		"img.png",
		// 其他你需要的图像
	}
	opts := mqtt.NewClientOptions().AddBroker("tcp://localhost:1883")
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	for _, image := range images {
		fmt.Println("Publishing " + image)
		payload, err := os.ReadFile(image)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if token := client.Publish(TOPIC, 0, false, payload); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
		} else {
			fmt.Println("Published " + image)
		}
		time.Sleep(1 * time.Second)
	}
	client.Disconnect(0)
}



func Test_Any(t *testing.T) {

	tests := []struct {
		name string
		got  any
		want any
	}{
		{
			name: "test1",
			got:  []any{[]float32{1.9999883, 0.60734314}},
			want: []any{[]float32{1.9999883, 0.60734314}},
		},
		{
			name: "test2",
			got: func() []any {
				result := make([]any, 2)
				result[0] = []float32{1.9999883}
				result[1] = []float32{0.60734314}
				return result
			}(),
			want: []any{[]float32{1.9999883, 0.60734314}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !reflect.DeepEqual(tt.got, tt.want) {
				t.Errorf("testName = %s  Exec() got = %v, want %v", tt.name, tt.got, tt.want)
			}
		})
	}
}
