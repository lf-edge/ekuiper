# 使用 Python 函数插件运行 OpenVINO 算法

[LF Edge eKuiper](https://www.lfedge.org/projects/ekuiper/) 是一款边缘轻量级物联网数据分析/流软件，可在各种资源受限的物联网设备上运行。

[OpenVINO](https://www.intel.com/content/www/us/en/developer/tools/openvino-toolkit/overview.html) 是一个开源工具包，可以更轻松地实现“编写一次，随处部署”。
可用于转换和优化使用 TensorFlow、PyTorch 和 Caffe 等流行框架训练的模型，并支持跨多种英特尔硬件和环境（本地、设备、浏览器或云端）进行部署。
OpenVINO 的示例代码和模型参考 [工业表面缺陷检测实施参考](https://www.intel.cn/content/www/cn/zh/developer/articles/reference-implementation/industrial-surface-defect-detection.html)，用户可以从该链接获取更多详细信息。

通过集成 eKuiper 和 OpenVINO，用户可以更轻松地分析数据。
在本教程中，我们将引导您构建一种基于 eKuiper 和 OpenVINO 的表面分割缺陷检测方法。
完整的插件包（含完整的代码）可以在 [eKuiper 资源页](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/openvinoProject.zip)下载。

## 先决条件

在开始教程之前，请准备以下产品或环境。

1. 安装 Python 3.x 环境。默认情况下，eKuiper 的便携式插件将使用 _python_ 命令运行，您跟根据实际环境更新[配置文件](../../configuration/global_configurations.md#portable-plugin-configurations) 中的 Python 命令，例如 `python3`。
2. 通过 `pip install opencv-python==4.7.0.* openvino==2023.0.0 numpy==1.24.3` 安装 opencv-python、numpy 和 openvino 软件包。

## 插件开发

为了将 eKuiper 与 OpenVINO 集成，我们将开发一个自定义 eKuiper 函数插件并配合 eKuiper 规则使用。我们将创建 _inference_ 函数，其输入是base64编码的图像数据，输出是 json 字典数据，其中包含片段缺陷的数量、处理后的图像数据和推理成本。

开发功能插件分为三步：

1. 用Python实现业务逻辑并将其包装为 eKuiper 函数。
2. 按照插件格式打包相关文件。
3. 创建实现扩展接口（源、接收器或函数）的 Python 文件：
   - 编写 Python 缺陷检测函数
   - 将现有函数包装为 eKuiper 函数插件

### 实现业务逻辑

我们的目标函数需要将 base64 编码的图像数据作为输入参数，进行图像预处理，加载 OpenVINO 模型，调用 OpenVINO 进行推理，提取推理结果并输出。我们将通过标准的 Python 函数实现该业务逻辑。

推理函数将接收 base64 编码的图像数据并返回结果。

```python

def inference(file_bytes):
    ie = IECore()
    # Read OpenVINO IR files
    net = ie.read_network(model=cwd + "models/model.xml", weights=cwd + "models/model.bin")

    # Prepare input blobs
    input_blob = next(iter(net.input_info))
    output_blob = next(iter(net.outputs))

    # Read and pre-process input images
    n, c, h, w = net.input_info[input_blob].input_data.shape

    # Loading model to the plugin
    exec_net = ie.load_network(network=net, device_name="CPU")

    # Start sync inference

    t0 = time()
    img_str = base64.b64decode(file_bytes.encode("ascii"))
    ndarray = np.fromstring(img_str, np.uint8)
    frame = cv2.imdecode(ndarray, cv2.IMREAD_COLOR)  #BGR
    frame = cv2.resize(frame, (w, h))
    org_img = frame.copy()
    frame = frame.transpose((2, 0, 1))
    images = np.expand_dims(frame, axis=0)
    pred = exec_net.infer(inputs={input_blob: images})
    infer_time = ((time() - t0) * 1000)
    result = np.squeeze(pred[output_blob])
    thresh = 0.5
    result[result < thresh] = 0
    result[result > thresh] = 255
    result = result.astype(np.uint8)
    contours, hierarchy = cv2.findContours(result, cv2.RETR_TREE,
                                           cv2.CHAIN_APPROX_SIMPLE)

    pred_mask = np.zeros((h, w, 3))
    pred_mask[result < 0.5] = (0, 0, 0)
    pred_mask[result >= 0.5] = (255, 255, 255)

    base64_str = cv2.imencode('.jpg', np.hstack((org_img, pred_mask)))[1].tostring()
    base64_byte = base64.b64encode(base64_str)
    b64str = base64_byte.decode()

    result = {
        "inference time": infer_time,
        "defect": len(contours),
        "base64": b64str
    }

    return result
```

以上代码仅涉及业务逻辑，无需调用 eKuiper 的 SDK 即可进行测试。我们只需要确保输入和输出的类型可以转换为 JSON 格式。例如，如果返回值是 numpy 数组，则需要先将其转换为列表类型。开发人员可以将主要功能或单元测试添加到其业务逻辑文件或另一个文件中进行测试。例如，可以使用下面的main函数来测试上述业务逻辑。
确保 model.xml 和 model.bin 文件位于 models 目录中。

```python
# To test the logic
if __name__ == '__main__':
    file_name = "test.jpg"
    with open(file_name, "rb") as f:
        cwd = './'
        result = inference(base64.b64encode(f.read()))
        print(json.dumps(result))
```

该文件使用 `test.jpg` 图像文件作为输入，调用推理函数对其进行测试，并将结果转换为 json 字符串并打印，以便我们查看该功能是否按预期工作。

### 插件实现

和原生插件一样，Python 插件也需要实现相应的接口，包括 Source、Sink 和 Function 接口，[接口定义](../../extension/portable/python_sdk.md#development)与原生插件类似。

创建 `inference.py` 函数来封装上一节中实现的函数。从 eKuiper 的插件 SDK 中导入 Function 类并创建相应的实现类。 `validate` 函数用于验证参数； `is_aggregate` 用于定义函数是否为聚合函数。关键的实现在 `exec` 函数中。这里，我们以 eKuiper 流中的数据为参数，调用上面实现的逻辑，并将结果返回给 eKuiper。

```python
from typing import List, Any

from ekuiper import Function, Context

class InferenceFunc(Function):

    def __init__(self):
        pass

    def validate(self, args: List[Any]):
        if len(args) != 1:
            return "invalid arg length"
        return ""

    def exec(self, args: List[Any], ctx: Context):
        logging.debug("executing inference")
        return inference(args[0])

    def is_aggregate(self):
        return False


inferenceIns = InferenceFunc()
```

代码实现后，我们还需要为每个函数添加一个描述文件，该描述文件放置在 functions 目录中，在本例中我们创建 `defect.json` 文件。

### 插件打包

至此，我们已经完成了主要功能的开发，接下来我们需要将这些文件打包成插件格式：

1. **管理依赖项**：如果插件有其他依赖项，则需要创建依赖项安装脚本 install.sh。安装插件时，eKuiper 会在插件包中查找安装脚本文件 install.sh，如果有则执行。在本例中，我们创建一个列出所有依赖包的 “requirements.txt” 文件。依赖项的安装是通过调用 `pip install -r $cur/requirements.txt` 在 `install.sh` 中实现。对于其他插件，如无特殊要求，可以重复使用此脚本来更新 `requirements.txt`。
2. **创建 Python 入口文件**：由于可以在单个插件中实现多个扩展，因此您需要一个入口文件来定义每个扩展的实现类。内容是一个主函数，它是插件运行时的入口点。它调用 SDK 中的方法来定义插件，包括插件名称以及插件中实现的源、接收器和函数的键列表。这里只实现了一个名为 `inference` 的函数插件，其对应的实现方法为 `inferenceIns`。 Python 插件进程独立于 eKuiper 主进程。

   ```python
   if __name__ == '__main__':
       # Define the plugin
       c = PluginConfig("defect", {}, {},
                        {"inference": lambda: inferenceIns})
       # Start the plugin instance
       plugin.start(c)
   ```

3. **创建 JSON 格式的插件描述文件**：用于定义插件的元数据。文件名必须与插件名称相同，即 `defect.json`。定义的函数名必须与入口文件完全对应，其中，`executable` 字段用于定义插件可执行入口文件的名称。

```json
{
  "version": "v1.0.0",
  "language": "python",
  "executable": "main.py",
  "sources": [],
  "sinks": [],
  "functions": ["inference"]
}
```

至此我们就完成了插件的开发，接下来我们只需要将目录下的所有文件打包成一个 zip 文件即可。zip 文件的文件结构应类似于：

- inference.py
- requirements.txt
- install.sh
- main.py
- defect.json
- models
  - model.bin
  - model.xml
- functions
  - defect.json

## 插件安装

与安装原生插件一样，我们也可以通过 REST API 安装 Python 插件。要使用 REST API，请将上面打包的 zip 文件上传到 eKuiper 所在的机器上，并通过以下 API 安装：

```text
### Install pyai plugin
POST http://{{host}}/plugins/functions
Content-Type: application/json

{"name":"defect", "file": "file:///tmp/defect.zip"}
```

安装时需要下载包括 `ffmpeg libsm6 libxext6`等依赖项，根据网络情况，下载可能需要一段时间。

## 运行插件

安装插件后，我们可以配合 eKuiper 规则使用。我们将创建一个规则来从 MQTT 主题接收图像字节数据，并通过 OpenVINO 模型对图像进行推理。

### 定义流

通过 eKuiper Rest API 定义流。我们创建一个名为 openvino_demo 的流，主题为 openvino_demo。

```shell
POST http://{{host}}/streams
Content-Type: application/json

{"sql":"CREATE STREAM openvino_demo () WITH (DATASOURCE=\"openvino_demo\")"}
```

### 创建规则

通过 eKuiper Rest API 定义规则。我们将创建一个名为 ruleOp 的规则。我们只是从 openvino_demo 流中读取 base64 编码的图像，并针对它运行自定义函数 _inference_。
当段缺陷数不为零时，它将发送 Base64 编码的原始图像和处理后的图像到主题 _ekuiper/defect_。

```shell
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "ruleOp",
  "sql": "SELECT image as origin, inference(image)->base64 as afterProcess from openvino_demo WHERE inference(image)->defect >=0",
  "actions": [
   {
      "mqtt":{
        "server": "tcp://emqx.io:1883",
        "sendSingle": true,
        "topic": "ekuiper/defect"
      }
    }
  ]
}
```

### 发送测试数据

这里我们创建一个 Python 程序，将图像数据发送到 openvino_demo 主题以由规则处理。
用户可以在[此处]获取完整代码(https://github.com/lf-edge/ekuiper/blob/master/docs/resources/openvinoProject.zip)

```python
def publish(client):
    topic = "openvino_demo"
    msg_count = 1
    while True:
        time.sleep(1)
        encoded = base64.b64encode(open('./1.png', 'rb').read()).decode()
        # open('1.txt', 'wb').write(encoded)  # 保存

        request_data = {
            "image": encoded,
        }

        payload = json.dumps(request_data)
        result = client.publish(topic, payload)
        # result: [0, 1]
        status = result[0]
        if status == 0:
            print(f"Send payload to topic `{topic}`")
        else:
            print(f"Failed to send message to topic {topic}")
        msg_count += 1
        if msg_count > 5:
            break
```

### 查看结果

用户可订阅 _ekuiper/defect_ 主题，并在图像出现片段缺陷时收到通知。

## 总结

本教程演示了如何通过自定义 eKuiper Python 插件，实现通过 OpenVINO 模型对实时图像流的缺陷分割。开发者可以将模型替换为自己想要的模型，构建自定义插件。
