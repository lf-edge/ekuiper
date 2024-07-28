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
	"os"
	"os/exec"
	"sync"
)

type mnist struct {
	modelPath         string
	once              sync.Once
	inputShape        ort.Shape
	outputShape       ort.Shape
	sharedLibraryPath string
}

func (f *mnist) Validate(args []interface{}) error {
	if len(args) != 1 {
		return fmt.Errorf("labelImage function only supports 1 parameter but got %d", len(args))
	}
	return nil
}

func (f *mnist) Exec(_ api.FunctionContext, args []any) (any, bool) {
	// This line _may_ be optional; by default the library will try to load
	// "onnxruntime.dll" on Windows, and "onnxruntime.so" on any other system.
	// For stability, it is probably a good idea to always set this explicitly.
	f.once.Do(func() {
		ort.SetSharedLibraryPath(f.sharedLibraryPath)

		err := ort.InitializeEnvironment()
		if err != nil {
			println("Failed to initialize environment: %s", err.Error())
		}
		checkFileStat(f.sharedLibraryPath)
	})
	var networkPath = f.modelPath
	var returnRes = ""

	inputs, outputs, err := ort.GetInputOutputInfo(networkPath)
	if err != nil {
		//return fmt.Sprintf("Error getting input and output info for %s: %w", networkPath, err) + printCurrDIr(), true
		return fmt.Sprintf("Error getting input and output info for %s: %w", networkPath, err), true
	}
	returnRes += fmt.Sprintf("%d inputs to %s:\n", len(inputs), networkPath)
	for i, v := range inputs {
		returnRes += fmt.Sprintf("  Index %d: %s\n", i, &v)
	}
	returnRes += fmt.Sprintf("%d outputs from %s:\n", len(outputs), networkPath)
	for i, v := range outputs {
		returnRes += fmt.Sprintf("  Index %d: %s\n", i, &v)
	}
	return returnRes, true
}

func (f *mnist) IsAggregate() bool {
	return false
}

var Mnist = mnist{
	modelPath:         "./data/functions/mnist/mnist_float16.onnx",
	sharedLibraryPath: "./data/functions/mnist/onnxruntime.so",
	//sharedLibraryPath: "/home/swx/GolandProjects/ekuiper/_build/kuiper-2.0.0-alpha.3-199-gaf747de4-linux-amd64/data/functions/mnist/onnxruntime_arm64.so",
	inputShape:  ort.NewShape(2, 5),
	outputShape: ort.NewShape(2, 3, 4),
}

var _ api.Function = &mnist{}

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
