# 使用外部函数运行 AI 算法

[LF Edge eKuiper](https://www.lfedge.org/projects/ekuiper/) 是一款边缘轻量级物联网数据分析/流软件，可在各种资源受限的物联网设备上运行。

[TensorFlow Lite](https://www.tensorflow.org/lite/guide) 是一组帮助开发人员在移动端、嵌入式和物联网设备上运行 TensorFlow
模型的工具，它使得设备上的机器学习预测具有低延迟和较小的二进制容量。

通过集成 eKuiper 和 TensorFlow Lite，用户可以使用预建的 TensorFlow 模型通过 AI 分析流中的数据。
在本教程中，我们将通过预先训练的图像识别 TensorFlow 模型, 来带您构建一个 eKuiper 外部函数插件来标记边缘生成的图片。
通过使用外部函数，eKuiper和外部函数可以运行在完全不同的进程或主机中，这意味着 eKuiper 和外部函数可以有不同的生命周期，更重要的是，外部函数
可以为除 eKuiper 以外的其他系统程序提供服务。


## 先决条件

外部功能插件将是一个 gRPC 服务器，因此用户应该了解 gRPC。本教程将给出设置 gRPC 服务器的示例代码。
用户可以在[这里](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/pythonGRPC.zip)下载示例代码。

用户还需要具备 Docker 的基本知识。

## 开发外部函数

在示例代码中，gRPC 服务器提供了 ``label`` 方法，用户只需要编写一个接口描述文件，注册到 eKuiper 中即可。然后eKuiper就可以调用 gRPC 方法,
就像内置函数一样。 ``label`` 方法由 ``tflite_runtime`` 图像分类提供支持，有关更多详细信息，请查看示例代码中的 `label.py` 文件。

这是提供服务的外部功能的 proto 文件描述。 ``label`` 方法的参数应该是 base64 编码的图像。

```proto
syntax = "proto3";

package sample;

// The algorithms service definition.
service Algorithms {
  rpc label(LabelRequest) returns(LabelReply) {}
}

// The request message containing the base64 encoded image.
message LabelRequest {
  string base64_img = 1;
}

message LabelResult {
  float  confidence = 1;
  string label = 2;
}

// The response message containing the greetings
message LabelReply {
  repeated LabelResult results = 1;
}
```

## 构建和运行 gRPC Server

我们提供 Dockerfile 来构建 gRPC 服务器，进入[示例代码](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/pythonGRPC.zip) pythonGRPC.zip 的根路径，运行以下命令来构建 gRPC docker 镜像

```shell
 docker build  -t test:1.1.1 -f deploy/Dockerfile-slim-python .
```

然后通过以下命令来启动服务

```shell
 docker run -d  -p 50051:50051 --name rpc-test test:1.1.1
```

现在，gRPC 服务器在 50051 端口上提供服务。

## 打包并注册外部函数

### 打包

将 gRPC Server 中服务的 json 描述文件和 proto 文件打包成 zip。 zip 文件中的文件结构应如下所示：

- schemas
  - sample.proto
- sample.json

- 有关文件格式和内容的更多详细信息，请参阅[这里](../../extension/external/external_func.md)。


您可以在[示例代码](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/pythonGRPC.zip)的文件夹 ``kuiper_package`` 中获取示例 zip 文件。


### 注册外部函数

将 sample.zip 文件放到 eKuiper 同一台机器的 /tmp 目录下，通过 cli 注册

```shell
# bin/kuiper create service sample '{"name": "sample","file": "file:///tmp/sample.zip"}'
```

## 使用外部函数

一旦注册了外部函数，我们就可以在我们的规则中使用它。我们将创建一个规则来从 mqtt 主题接收图像数据，并通过 tflite 模型标记图像。

### 创建流

通过 eKuiper Cli 创建流。我们创建一个名为 demo 的 mqtt 流，它订阅主题 “tfdemo”。

```shell
#/bin/kuiper create stream demo '() with (DATASOURCE="tfdemo")'
```

### 创建规则

通过 eKuiper cli 创建规则。我们将创建一个查询。我们从 demo 流中读取 base64 编码的图像，并针对它进行自定义函数 label 运算, 其结果将是 AI 识别的图像标签。
```shell
#/bin/kuiper query

Connecting to 127.0.0.1:20498... 
kuiper >  select label(image) from demo

```

### 测试数据

用户需要像这样以 json 格式发送数据

```json
{"image": "base64 encoded data"}
```

用户可以从 ``images/example.json`` 文件中的示例代码中获取真实数据，只需通过 MQTT 客户端将其发送到 MQTT Broker 即可。

### 查询结果

发布 base64 编码的图像后，您可以得到结果。

```shell
kuiper > [{"label":{"results":[{"confidence":0.5789139866828918,"label":"tailed frog"},{"confidence":0.3095814287662506,"label":"bullfrog"},{"confidence":0.040725912898778915,"label":"whiptail"},{"confidence":0.03226377069950104,"label":"frilled lizard"},{"confidence":0.01566782221198082,"label":"agama"}]}}]
```

## 总结

在本教程中，我们将引导您构建外部函数以利用预训练的 TensorFlowLite 模型。如果您需要使用其他 gRPC 服务，只需按照创建自定义函数的步骤操作即可。在边缘设备中享受 AI吧。