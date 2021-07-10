# 使用 eKuiper 函数插件运行TensorFlow Lite 模型

[LF Edge eKuiper](https://docs.emqx.io/en/kuiper/latest/) 是一款边缘轻量级物联网数据分析/流软件，可在各种资源受限的物联网设备上运行。

[TensorFlow Lite](https://www.tensorflow.org/lite/guide) 是一组帮助开发人员在移动端、嵌入式和物联网设备上运行 TensorFlow 模型的工具，它使得设备上的机器学习预测具有低延迟和较小的二进制容量。

通过集成 eKuiper 和 TensorFlow Lite，用户可以通过包含预先构建的 TensorFlow 模型的AI分析流中的数据。 在本教程中，我们将引导您构建一个 eKuiper 插件，通过预先训练的图像识别 TensorFlow 模型，标记边缘设备生成的流图片（二进制数据）。

## 先决条件

如需运行 TensorFlow Lite 解释器，我们需要一个经过训练的模型。在本教程中，我们将不介绍如何训练和涵盖这个模型，您可以通过查看 [tflite converter](https://www.tensorflow.org/lite/convert) 了解如何做到这一点。我们既可以训练一个新的模型，也可以在线选择一个。在本教程中，我们将使用 [mattn/go tflite](https://github.com/mattn/go-tflite) 的 [label image](https://github.com/mattn/go-tflite/tree/master/_example/label_image) 模型。该 repo为 tflite C API 创建了 golang 绑定。 我们还将使用它来实现我们的插件。

## 开发插件

为了集成 eKuiper 和 TensorFlow Lite，我们将开发一个定制的 eKuiper 函数插件，供 eKuiper 规则使用。例如，我们将创建 `LabelImage`  函数，其输入是表示图像的二进制类型数据，输出是表示图像标签的字符串。例如，如果输入图像中有孔雀，`LabelImage(col)` 将输出“孔雀”。

要开发函数插件，我们需要：

1. 创建插件 go 文件。 例如，在 eKuiper 源代码中，创建 *plugins/functions/labelImage/labelImage.go* 文件。
2. 创建一个实现 [api.函数接口](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) 的 struct。
3. 导出 struct。

实现的关键是 *Exec* 函数。 伪代码如下：

```go
func (f *labelImage) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	
    //... 初始化和验证
    
    // 解码输入图像
	img, _, err := image.Decode(bytes.NewReader(arg[0]))
	if err != nil {
		return err, false
	}
	var outerErr error
	f.once.Do(func() {		
		// 加载标签、tflite模型并初始化tflite解释器
	})

	// 对输入图像运行解释器
	
    // 返回可能性最大的标签
    return result, true
}
```

要注意的另一件事是插件的导出。 该函数是无状态的，因此我们将仅导出一个 struct 实例。 所有使用此函数的规则都会共享一个实例，以避免创建实例和加载模型的开销。 模型和标签路径将在实例化时指定。

```go
var LabelImage = labelImage{
	modelPath: "labelImage/mobilenet_quant_v1_224.tflite",
	labelPath: "labelImage/labels.txt",
}
```

查阅 [本教程](../plugins_tutorial.md) 以获得创建 eKuiper 插件的详细步骤。请参阅 [labelImage.go](https://github.com/lf-edge/ekuiper/blob/master/extensions/functions/labelImage/labelImage.go) 以获取完整的源代码。

## 构建并安装插件

要使用该插件，我们需要在运行 eKuiper 的环境中对其进行构建，然后将其安装在 eKuiper 中。

### 通过预构建的  zip 安装

如果使用基于 debian 的带有 1.1.1 或 1.1.1-slim标签的 eKuiper docker 镜像，我们可以安装预构建的 labelImage插件。 例如，要在 docker image lfedge/ekuiper:1.1.2-slim 中安装 eKuiper 1.1.2 插件，则预构建的 zip 文件位于 *https://www.emqx.io/downloads/kuiper-plugins/v1.1.2/debian/functions/labelImage_amd64.zip*。 按如下所示运行 rest命令以进行安装。

```shell
POST http://{{kuiperHost:kuiperRestPort}}/plugins/functions
Content-Type: application/json

{"name":"labelImage", "file": "https://www.emqx.io/downloads/kuiper-plugins/v1.1.2/debian/functions/labelImage_amd64.zip"}
```

### 手动构建

如果您不使用官方的 eKuiper docker 镜像运行 eKuiper，由于 golang 插件的限制，预构建的 labelImage 插件将不适用。您需要手动构建插件。手动创建插件 zip 文件有3个步骤：

1. 构建 TensorFlowLite C API。
2. 构建 labelImage 插件。
3. 将插件与安装脚本打包在一起。

#### 构建 TensorFlowLite C API

有一个来自 tensorflow repo 的关于构建C API的非常简单的 [说明](https://github.com/tensorflow/tensorflow/tree/v2.2.0-rc3/tensorflow/lite/c) 。 我们将在本节中逐步详细展开。 请注意，该插件仅针对 TensorFlow v2.2.0-rc3 进行测试，因此我们将以此版本为基础进行构建。 以 **ubuntu** 为例，以下是构建步骤：

1. 安装 [Python 3](https://www.tensorflow.org/install/pip#1.-install-the-python-development-environment-on-your-system).

2. 将 [requirements.txt](requirements.txt) 复制到您指定位置。 安装所需的 python 库：`pip3 install -r requirements.txt`。 requirements 来自相应 TensorFlow 版本的 `tensorflow/tensorflow/tools/pip_package/setup.py`。

3. 安装 TensorFlow 的构建工具 [Bazel](https://docs.bazel.build/versions/4.0.0/install-ubuntu.html)。

4. 克隆 [tesorflow repo](https://github.com/tensorflow/tensorflow)，通过 `git checkout v2.2.0-rc3 -b mybranch` 命令切换到所需的分支。

5. 生成目标 .so 文件，输出将位于 ./bazel-bin 中。 将两个 so 文件复制到 tensorflow/lib 文件夹中。

   ```bash
   $ cd $tensorflowSrc
   $ bazel build --config monolithic -c opt //tensorflow/lite:libtensorflowlite.so
   $ bazel build --config monolithic -c opt //tensorflow/lite/c:libtensorflowlite_c.so
   $ mkdir lib
   $ cp bazel-bin/tensorflow/lite/libtensorflowlite.so lib
   $ cp bazel-bin/tensorflow/lite/c/libtensorflowlite_c.so lib
   ```

6. 安装 so 文件。
   1. 更新 ldconfig 文件 `sudo vi / etc / ld.so.conf.d / tflite.conf`。
   2. 将路径  `{{tensorflowPath}}/lib` 添加到 tflite.conf，然后保存并退出。
   3. 运行 ldconfig: `sudo ldconfig`。
   4. 检查安装结果：`ldconfig -p | grep libtensorflow`。 确保列出了两个so文件。

#### 构建 labelImage 插件

确保已克隆 eKuiper github repo。 插件源文件位于 *extensions/functions/labelImage/labelImage.go* 中。 在构建插件之前，导出 tensorflow repo 和构建库的路径。

```shell
$ cd {{eKuiperRepoPath}}
$ export CGO_CFLAGS=-I/root/tensorflow
$ export CGO_LDFLAGS=-L/root/tensorflow/lib
$ go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/functions/LabelImage.so extensions/functions/labelImage/*.go
$ mkdir -p "plugins/functions"
$ cp -r extensions/functions/labelImage plugins/functions
```

通过这些命令，插件将构建到 plugins/functions/LabelImage.so 中，同时复制所有依赖文件到 plugins/functions/labelImage 目录下。 出于开发目的，您可以重新启动 eKuiper 以自动加载此插件并进行测试。 测试完成后，我们应该将其打包为一个 zip 文件，该文件可供 eKuiper 插件安装API 使用，以便可以在其他计算机（例如生产环境）中使用。

#### 打包插件

将 *plugins/functions/labelImage* 目录中的所有文件和目录与构建的 LabelImage.so 一起打包到一个 zip 文件中。 zip文件的文件结构应类似于：

- etc
    - labels.txt
    - mobilenet_quant_v1_224.tflite
- lib
    - libtensorflowlite.so
    - libtensorflowlite_c.so
- install.sh
- LabelImage.so
- tflite.conf

将打包的插件安装到目标系统，如 [通过预构建 zip 安装](#install-by-pre-built-zip) 所示。

## 运行插件

插件安装后，我们就可以在规则中使用它了。 我们将创建一个规则用于接收来自 mqtt 主题的图像字节数据，并通过 tflite 模型标记该图像。

### 定义流

通过 eKuiper rest API 定义流。 我们创建一个名为 tfdemo 的流，其格式为二进制，主题为 tfdemo。

```shell
POST http://{{host}}/streams
Content-Type: application/json

{"sql":"CREATE STREAM tfdemo () WITH (DATASOURCE=\"tfdemo\", FORMAT=\"BINARY\")"}
```

### 定义规则

通过 eKuiper rest API 定义规则。 我们将创建一个名为 ruleTf 的规则。 我们只是从 tfdemo 流中读取图像，然后对其运行自定义函数 *labelImage*。 返回结果将是 AI 识别的图像的标签。

```shell
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "ruleTf",
  "sql": "SELECT labelImage(self) FROM tfdemo",
  "actions": [
    {
      "log": {}
    }
  ]
}
```

### 输入数据

在这里，我们创建了一个 go 程序，用于将图像数据发送到 tfdemo 主题以便由规则进行处理。

```go
package main

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"io/ioutil"
	"time"
)

func main(){
	const TOPIC = "tfdemo"

	images := []string{
		"peacock.png",
		"frog.jpg",
        // 其他你需要的图像
	}
	opts := mqtt.NewClientOptions().AddBroker("tcp://yourownhost:1883")
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}
	for _, image := range images {
		fmt.Println("Publishing " + image);
		payload, err := ioutil.ReadFile(image)
		if err != nil{
			fmt.Println(err)
			continue
		}
		if token := client.Publish(TOPIC, 0, false, payload); token.Wait() && token.Error() != nil {
			fmt.Println(token.Error())
		} else {
			fmt.Println("Published " + image);
		}
		time.Sleep(1 * time.Second)
	}
	client.Disconnect(0)
}

```

运行 pub.go，它将开始将图像输入 tfdemo 主题。

### 检查结果

因为我们的规则定义只有一个目标：log，所以结果将被写入日志文件。 我们用 *peacock.png* 和 *frog.png* 两个图像填充流。 检查日志文件，我们会发现：

```shell
time="2021-02-05 16:23:29" level=info msg="sink result for rule ruleTf: [{\"labelImage\":\"peacock\"}]" file="sinks/log_sink.go:16" rule=ruleTf
time="2021-02-05 16:23:30" level=info msg="sink result for rule ruleTf: [{\"labelImage\":\"bullfrog\"}]" file="sinks/log_sink.go:16" rule=ruleTf
```

图像标记正确。

## 结论

在本教程中，我们将引导您构建自定义的 eKuiper 插件，以利用预先训练好的 TensorFlow Lite 模型。 如果需要使用其他模型，只需按照规定步骤创建另一个函数。 请注意，如果在同一环境中运行，构建的 TensorFlow C API 可以在所有函数之间共享。希望这些功能能让你在实现边缘设备中的AI时候感到开心 。