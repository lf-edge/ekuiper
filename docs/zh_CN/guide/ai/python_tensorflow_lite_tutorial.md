# 使用 Python 函数插件运行 AI 算法

[LF Edge eKuiper](https://www.lfedge.org/projects/ekuiper/) 是一款边缘轻量级物联网数据分析/流软件，可在各种资源受限的物联网设备上运行。

[TensorFlow Lite](https://www.tensorflow.org/lite/guide) 是一组帮助开发人员在移动端、嵌入式和物联网设备上运行 TensorFlow
模型的工具，它使得设备上的机器学习预测具有低延迟和较小的二进制容量。

通过集成 eKuiper 和 TensorFlow Lite，用户可以通过包含预先构建的 TensorFlow 模型分析流中的数据。 在本教程中，我们将引导您构建一个 eKuiper 插件，通过预先训练的图像识别 TensorFlow
模型，标记边缘设备生成的流图片（二进制数据）。在早前的[教程](./tensorflow_lite_tutorial.md)，我们采用了 GO 语言原生插件的方式实现了模型调用。在本教程种，我们将使用 Python 插件实现类似的功能。

教程完成的插件包可在[此处](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/pyai.zip)下载，其中也包含了完整的源代码。

## 先决条件

如需运行 TensorFlow Lite 解释器，我们需要一个经过训练的模型。在本教程中，我们将不介绍如何训练和涵盖这个模型，您可以通过查看 [tflite converter](https://www.tensorflow.org/lite/convert) 了解如何做到这一点。我们既可以训练一个新的模型，也可以在线选择一个。在本教程中，我们将使用 [TensorFlow 图像分类示例](https://www.tensorflow.org/lite/examples/image_classification/overview) 中的图像分类模型。

开始教程之前，请准备以下产品或环境：
1. 安装 Python 3.x 环境。
2. 通过 `pip install pynng ekuiper tflite_runtime` 安装 pynng，ekuiper 和 tensorflow lite 包。

默认情况下，eKuiper 的 portable 插件运行时会通过 `python` 命令来运行插件。如果您的环境不支持 `python` 命令，请通过[配置文件](../../configuration/global_configurations.md#portable-插件配置)更换为可用的 Python 命令，如 `python3`。

若使用 Docker 进行开发，可使用 `lfedge/ekuiper:<tag>-slim-python` 版本。该版本包含 eKuiper 和 python 环境，无需再手动安装。

## 开发插件

为了集成 eKuiper 和 TensorFlow Lite，我们将开发一个定制的 eKuiper 函数插件，供 eKuiper 规则使用。例如，我们将创建 `labelImage`  函数，其输入是表示图像的二进制类型数据，输出是表示图像标签的字符串。所以，如果输入图像中有孔雀，`labelImage(col)` 将输出 `peacock`。

要开发函数插件，我们需要：

1. 通过 Python 实现业务逻辑并包装为 eKuiper 函数。
2. 按照插件格式对相关文件进行打包。

创建 Python 文件实现扩展接口（源，sink 或函数）。在本例中，我们要开发的是函数插件， 因此需要实现函数扩展接口。
- 编写 Python 图像分类函数
- 包装已有函数为 eKuiper 函数插件


### 实现业务逻辑

我们的目标函数希望接收一个图像的二进制数据作为输入参数，在函数中进行图像的预处理等操作，调用 TensorFlow Lite 模型进行推理，从推理结果中提取出可能性最大的分类结果并输出。我们需要使用 Python ，跟编写普通的 Python 函数相同的方式实现这个函数。

1. 下载[图像分类模型](https://storage.googleapis.com/download.tensorflow.org/models/tflite/mobilenet_v1_1.0_224_quant_and_labels.zip)，解压后放置在插件项目中。里面包含一个模型文件 `mobilenet_v1_1.0_224.tflite` 和分类文本文件 `labels.txt`。
2. 实现图像分类推理业务逻辑。创建一个 Python 文件 label.py, 在其中实现函数 `label(file_bytes)`。

label 函数将接收由 eKuiper 规则传过来的 base64 编码的图像数据并进行推理分类。其实现的伪代码如下：

```python
def label(file_bytes):
    # 载入模型文件
    interpreter = tf.Interpreter(
        model_path= cwd + 'mobilenet_v1_1.0_224.tflite')
    
    # 预处理输入图片，将其转换为输入的 tensor 格式，此处代码省略
    
    # 设置模型输入，调用推理，拿到结果 tensor
    interpreter.set_tensor(input_details[0]['index'], input_data)
    interpreter.invoke()
    output_data = interpreter.get_tensor(output_details[0]['index'])
    
    # 对结果进行后处理，转换为输出格式，代码省略
    return result
```

以上代码仅跟业务逻辑相关，无需调用 eKuiper 的 SDK 即可进行测试。我们只需要保证输入输出为可转为 JSON 格式的类型即可。例如，若返回值为 numpy array，需先转换成 list 类型。开发者可在自己的业务逻辑文件或另外的文件中添加 main 函数或单元测试进行测试。例如，可采用以下 main 函数测试以上业务逻辑。

```python
# To test the logic
if __name__ == '__main__':
    file_name = "peacock.jpg"
    with open(file_name, "rb") as f:
        cwd = './'
        result = label(base64.b64encode(f.read()))
        print(json.dumps(result))
```

该文件使用了 `peacock.jpg` 图像文件作为输入，调用了 label 函数进行测试，并将结果转换为 json 字符串并打印。从而可以观察函数运行结果是否符合预期。此处，我们应当得到一个 json 数组，按照置信度的高低对识别结果进行排序。

```json
[{"confidence": 0.9999935626983643, "label": "85:peacock"}, {"confidence": 2.156877371817245e-06, "label": "8:cock"}, {"confidence": 1.5930896779536852e-06, "label": "81:black grouse"}, {"confidence": 9.999589565268252e-07, "label": "92:coucal"}, {"confidence": 7.304166160793102e-07, "label": "96:jacamar"}]
```

详细信息请参见完整代码中的 `lable.py` 文件。

### 插件实现

与原生插件相同，Python 插件也需要实现对应的接口。Python 插件也支持 Source, Sink 和 Function 接口，[接口定义](../../extension/portable/python_sdk.md#插件开发)与原生插件类似。此处，我们需要实现的是函数接口。

创建 `label_func.py` 函数对上节实现的函数进行封装。导入 eKuiper 的插件 SDK 中的 Function 类，创建对应的实现类。其中，validate 函数用于参数的验证；is_aggregate 用于定义函数是否为聚合函数。关键的实现都在 exec 函数中。此处，我们将 eKuiper 流中的数据作为参数，调用上文实现的逻辑，并将结果返回到 eKuiper 中。

注意此处导入的 eKuiper python SDK 版本应与最终运行的 eKuiper 版本形同。

```python
from typing import List, Any

from ekuiper import Function, Context

from label import label

# 继承 SDK 中的 Function 类，并实现其中的方法
class LabelImageFunc(Function):

    def __init__(self):
        pass

    def validate(self, args: List[Any]):
        if len(args) != 1:
            return "invalid arg length"
        return ""

    def exec(self, args: List[Any], ctx: Context):
        return label(args[0])

    def is_aggregate(self):
        return False

# 创建类的一个对象，供入口函数调用
labelIns = LabelImageFunc()
```

代码实现完成后，我们还需要给每个函数添加一个描述文件，置于 functions 目录下。本插件只实现了一个函数，因此只需要创建 `labelImage.json` 文件。注意，文件名为函数的名字，而非插件的名字。该文件可以帮助 eKuiper manager 自动生产插件相关的 UI。

### 插件打包

至此，我们已经完成了主要功能的开发，接下来需要将这些文件打包成插件的格式。插件打包需要完成几个步骤：

1. 如果插件有额外的依赖，例如本例中的 TensorFlow Lite, 需要创建依赖安装脚本 `install.sh`。插件安装时，eKuiper 会查找插件包中是否有安装脚本文件 `install.sh`，若有的话执行安装脚本。在本例中，我们创建一个 `requirements.txt` 文件列出所有的依赖包。在 `install.sh` 通过调用 `pip install -r $cur/requirements.txt` 完成依赖的安装。对于别的插件，若无特殊要求可重用该脚本，更新`requirements.txt`即可。
2. 创建 Python 入口文件，用于暴露所有实现的接口。因为在单个插件中可以实现多个扩展，所以需要一个入口文件定义各个扩展的实现类。其内容为一个 main 函数，为插件运行时入口。它调用 SDK 里的方法定义插件，包括插件名，插件里实现的 source, sink, function 的键值列表。此处仅实现一个名为 `labelImage` 的函数插件，其对应的实现方法为 `labelIns`。之后调用 start 方法启动插件进程的运行。Python 插件进程独立于 eKuiper 主进程。
    ```python
    if __name__ == '__main__':
        # 定义插件
        c = PluginConfig("pyai", {}, {},
                         {"labelImage": lambda: labelIns})
        # 启动插件
        plugin.start(c)
    ```
3. 创建 JSON 格式的插件描述文件，用于定义插件的元数据。文件名必须与插件名相同，即 `pyai.json`。其中定义的函数名与入口文件必须完全对应，文件内容如下。其中，executable 用于定义插件的可执行入口文件名。
    ```json
    {
      "version": "v1.0.0",
      "language": "python",
      "executable": "pyai.py",
      "sources": [
      ],
      "sinks": [
      ],
      "functions": [
        "labelImage"
      ]
    }
    ```

至此我们已经完成了插件的开发，接下来只需要把目录中的所有文件打包成 zip 文件即可。 zip文件的文件结构应类似于：

- label.py
- label_func.py
- requirements.txt
- model.tflite
- install.sh
- pyai.py
- pyai.json
- functions
  - labelImage.json

## 插件安装

与安装原生插件相同，我们也可以通过管理控制台或者 REST API 进行 Python 插件的安装。使用 REST API 的时候，把上文打包的 zip 文件上传到 eKuiper 所在的机器中。然后使用如下 API 进行安装。

```text
### Install pyai plugin
POST http://{{host}}/plugins/portables
Content-Type: application/json

{"name":"pyai", "file": "file:///tmp/pyai.zip"}
```

安装过程中需要联网下载依赖，包括 `tflite_runtime`，视网络情况可能安装过程需要较长时间。

## 运行插件

插件安装后，我们就可以在规则中使用它了。 我们将创建一个规则用于接收来自 MQTT 主题的图像字节数据，并通过 tflite 模型标记该图像。

### 定义流

通过 eKuiper rest API 定义流。 我们创建一个名为 tfdemo 的流，其格式为二进制，主题为 tfdemo。

```shell
POST http://{{host}}/streams
Content-Type: application/json

{"sql":"CREATE STREAM tfdemo () WITH (DATASOURCE=\"tfdemo\", FORMAT=\"BINARY\")"}
```

### 定义规则

通过 eKuiper rest API 定义规则。 我们将创建一个名为 ruleTf 的规则。 我们只是从 tfdemo 流中读取图像，然后对其运行自定义函数 *labelImage*。 返回结果将是 AI 识别的图像的标签数组，包含按照置信度排名的标签。我们的规则取出其中第一个置信度最高的标签，并发送到 MQTT 主题 `ekuiper/labels`。

```shell
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "ruleTf",
  "sql": "SELECT labelImage(self)[0]->label as label FROM tfdemo",
  "actions": [
   {
      "mqtt":{
        "server": "tcp://emqx.io:1883",
        "sendSingle": true,
        "topic": "ekuiper/labels"
      }
    }
  ]
}
```

### 输入数据

在这里，我们创建了一个 go 程序，用于将图像数据发送到 tfdemo 主题以便由规则进行处理。模型接受 224x224 像素的图像输入。在插件中，我们对输入的图像进行了预处理，调整了图片大小，因此任意的图像输入都可以。一部分 MQTT 服务器默认配置限制了数据的大小，建议输入不大于 2 MB 的图像数据。

```go
package main

import (
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"io/ioutil"
	"time"
)

func main() {
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
		fmt.Println("Publishing " + image)
		payload, err := ioutil.ReadFile(image)
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

```

运行 pub.go，它将开始将图像输入 tfdemo 主题。请注意，大部分的 MQTT broker 不支持传输太大的图像文件。实际场景中，我们可以调整 MQTT broker 的大小限制或者使用别的 source 进行图像流的输入。

### 检查结果

因为我们的规则定义只有一个目标：MQTT，所以结果将写入MQTT 主题 `ekuiper/labels`。使用 MQTT 客户端订阅该主题，我们用 *peacock.png* 和 *frog.png* 两个图像输入 tfdemo 主题，我们将得到两个结果。

```shell
{"label":"85:peacock"}
{"label":"33:tailed frog, bell toad, ribbed toad, tailed toad, Ascaphus trui"}
```

图像标记正确。

## 结论

在本教程中，我们引导您构建自定义的 eKuiper Python 插件，以利用预先训练好的 TensorFlow Lite 模型，实现了实时图像流的分类标注功能。开发者可以将模型替换为自己所需的模型，实现自己的插件。