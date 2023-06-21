# Running AI Algorithms with Python Function Plugins

[LF Edge eKuiper](https://www.lfedge.org/projects/ekuiper/) is an edge lightweight IoT data analytics / streaming
software which can be run at all kinds of resource constrained IoT devices.

[TensorFlow Lite](https://www.tensorflow.org/lite/guide) is a set of tools to help developers run TensorFlow models on
mobile, embedded, and IoT devices. It enables on-device machine learning inference with low latency and a small binary
size.

By integrating eKuiper and TensorFlow Lite, users can analyze the data in stream by AI with prebuilt TensorFlow models.
In this tutorial, we will walk you through building a eKuiper plugin to label pictures (binary data) produced by an edge
device in stream by pre-trained image recognition TensorFlow model. In earlier [tutorial](./tensorflow_lite_tutorial.md), we have implemented the model inference using a GO language native plugin. In this tutorial, we will use a Python plugin to implement the similar functionality.

The completed plugin package can be downloaded [here](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/pyai.zip), which also contains the full source code.

## Prerequisite

To run the TensorFlow lite interpreter, we need a trained model. We won't cover how to train and cover a model in this tutorial, you can check [tflite converter](https://www.tensorflow.org/lite/convert) for how to do that. We can either train a new model or pick one online. In this tutorial, we will use the model in [TensorFlow image classification example](https://www.tensorflow.org/lite/examples/image_classification/overview).

Before starting the tutorial, please prepare the following products or environments.

1. Install the Python 3.x environment.
2. Install the pynng, ekuiper and tensorflow lite packages via `pip install pynng ekuiper tflite_runtime`.

By default, the portable plugin for eKuiper will run with the `python` command. If your environment does not support the `python` command, please use the [configuration file](../../configuration/global_configurations.md#portable-plugin-configurations) to modify the Python command, such as `python3`.

If you are developing with Docker, you can use the `lfedge/ekuiper:<tag>-slim-python` version. This version includes both the eKuiper and python environments.

## Develop the Plugin

To integrate eKuiper with TensorFlow Lite, we will develop a custom eKuiper function plugin to use with eKuiper rules. For example, we will create the `labelImage` function whose input is binary type data representing an image and whose output is a string representing the label of the image. So if the input image has a peacock in it, `labelImage(col)` will output `peacock`.

To develop the function plugin, we need to：

1. implement the business logic in Python and wrap it as an eKuiper function.
2. Package the relevant files according to the plugin format.

Create Python files that implement the extended interface (source, sink, or function). In this tutorial, we are developing a function plugin, so we need to implement the function extension interface.

- Writing Python image classification functions
- Wrapping an existing function as an eKuiper function plugin

### Implement the business logic

Our target function wants to take the binary data of an image as an input parameter, perform image preprocessing and other operations in the function, call the TensorFlow Lite model for inference, extract the most likely classification result from the inference result, and output it. We need to implement this function using Python in the same way as we would write a normal Python function.

1. Download the [Image Classification Model](https://storage.googleapis.com/download.tensorflow.org/models/tflite/mobilenet_v1_1.0_224_quant_and_labels.zip), unzip it, and place it in the plug-in project. It contains a model file `mobilenet_v1_1.0_224.tflite` and a classification text file `labels.txt`.
2. Implement the image classification inference business logic. Create a Python file label.py, and implement the function `label(file_bytes)` in it.

The label function will receive the base64 encoded image data passed by the eKuiper rule and perform inference on the classification. The pseudocode for its implementation is as follows:

```python
def label(file_bytes):
    # Load the model file
    interpreter = tf.Interpreter(
        model_path= cwd + 'mobilenet_v1_1.0_224.tflite')
    
    # Preprocess the input image, turn it into tensors. Here the code is omitted.
    
    # Set model input, call inference, get result tensor
    interpreter.set_tensor(input_details[0]['index'], input_data)
    interpreter.invoke()
    output_data = interpreter.get_tensor(output_details[0]['index'])
    
    # Post process the result and turn it into the output format. Here the code is omitted.
    return result
```

The above code is only related to the business logic and can be tested without calling eKuiper's SDK. We just need to make sure that the input and output is of a type that can be converted to JSON format. For example, if the return value is a numpy array, it needs to be converted to a list type first. Developers can add main functions or unit tests to their business logic file or to another file for testing. For example, the following main function can be used to test the above business logic.

```python
# To test the logic
if __name__ == '__main__':
    file_name = "peacock.jpg"
    with open(file_name, "rb") as f:
        cwd = './'
        result = label(base64.b64encode(f.read()))
        print(json.dumps(result))
```

This file uses the `peacock.jpg` image file as input, calls the label function to test it, and converts the result to a json string and prints it. This allows us to see if the function works as expected. Here, we should get a json array and sort the recognition results by confidence level.

```json
[{"confidence": 0.9999935626983643, "label": "85:peacock"}, {"confidence": 2.156877371817245e-06, "label": "8:cock"}, {"confidence": 1.5930896779536852e-06, "label": "81:black grouse"}, {"confidence": 9.999589565268252e-07, "label": "92:coucal"}, {"confidence": 7.304166160793102e-07, "label": "96:jacamar"}]
```

See the `lable.py` file in the full code for details.

### Plugin Implementation

Like native plugins, Python plugins need to implement the corresponding interfaces; Python plugins also support the Source, Sink and Function interfaces, [interface definition](../../extension/portable/python_sdk.md#development) is similar to the native plugins. Here, what we need to implement is the function interface.

Create the `label_func.py` function to wrap the functions implemented in the previous section. Import the Function class from eKuiper's plugin SDK and create the corresponding implementation class. The validate function is used to validate the parameters; is_aggregate is used to define whether the function is an aggregate function. The key implementations are in the exec function. Here, we take the data in the eKuiper stream as an argument, call the logic implemented above, and return the result to eKuiper.

Note that the version of the eKuiper python SDK imported here should be the same as the target running version of eKuiper.

```python
from typing import List, Any

from ekuiper import Function, Context

from label import label

# Inherit Function class from eKuiper SDK and implement it.
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

# Create an instance for the class to be called by the entry function later
labelIns = LabelImageFunc()
```

Once the code is implemented, we also need to add a description file for each function, which is placed in the functions directory. This plugin only implements one function, so you only need to create the `labelImage.json` file. Note that the file name is the name of the function and not the name of the plugin. This file will help the eKuiper manager to automatically produce the UI associated with the plugin.

### Plugin Packaging

At this point, we have completed the development of the main functionality, and next we need to package these files into a plugin format. Plugin packaging requires several steps to be completed.

1. If the plugin has additional dependencies, such as TensorFlow Lite in this case, you need to create the dependency installation script `install.sh`. When the plugin is installed, eKuiper will look for an installation script file `install.sh` in the plugin package and execute the it if there is one. In this case, we create a `requirements.txt` file listing all the dependency packages. The installation of the dependencies is done in `install.sh` by calling `pip install -r $cur/requirements.txt`. For other plugins, you can reuse this script to update `requirements.txt` if you have no special requirements.
2. Create a Python entry file that exposes all the implemented interfaces. Because multiple extensions can be implemented in a single plugin, you need an entry file that defines the implementation classes for each extension. The content is a main function, which is the entry point for the plugin runtime. It calls the methods in the SDK to define the plugin, including the plugin name, and a list of keys for the source, sink, and function implemented in the plugin. Here only a function plugin named `labelImage` is implemented, and its corresponding implementation method is `labelIns`. The Python plug-in process is independent of the eKuiper main process.

    ```python
    if __name__ == '__main__':
        # Define the plugin
        c = PluginConfig("pyai", {}, {},
                         {"labelImage": lambda: labelIns})
        # Start the plugin instance
        plugin.start(c)
    ```

3. Create a plugin description file in JSON format to define the metadata of the plugin. The file name must be the same as the plugin name, i.e. `pyai.json`. The function names defined therein must correspond exactly to the entry file, and the contents of the file are as follows. Where, executable is used to define the name of the plugin's executable entry file.

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

At this point we have completed the development of the plugin, next we just need to package all the files in the directory into a zip file. The zip file should have a file structure similar to:

- label.py
- label_func.py
- requirements.txt
- model.tflite
- install.sh
- pyai.py
- pyai.json
- functions
  - labelImage.json

## Plugin Installation

As the same with installing native plugins, we can also install Python plugins via the eKuiper manager UI or the REST API. To use the REST API, upload the zip file packaged above to the machine where eKuiper is located. Then use the following API to install it:

```text
### Install pyai plugin
POST http://{{host}}/plugins/portables
Content-Type: application/json

{"name":"pyai", "file": "file:///tmp/pyai.zip"}
```

The installation process requires an Internet connection to download dependencies, including `tflite_runtime`, which may take a long time depending on the network conditions.

## Run the plugin

Once the plugin installed, we can use it in our rule. We will create a rule to receive image byte data from a mqtt topic and label the image by tflite model.

### Define the stream

Define the stream by eKuiper rest API. We create a stream named tfdemo whose format is binary and the topic is tfdemo.

```shell
POST http://{{host}}/streams
Content-Type: application/json

{"sql":"CREATE STREAM tfdemo () WITH (DATASOURCE=\"tfdemo\", FORMAT=\"BINARY\")"}
```

### 定义规则

通过 eKuiper rest API 定义规则。 我们将创建一个名为 ruleTf 的规则。 我们只是从 tfdemo 流中读取图像，然后对其运行自定义函数 *labelImage*。 返回结果将是 AI 识别的图像的标签数组，包含按照置信度排名的标签。我们的规则取出其中第一个置信度最高的标签，并发送到 MQTT 主题 `ekuiper/labels`。

### Define the rule

Define the rule by eKuiper rest API.  We will create a rule named ruleTf. We just read the images from tfdemo stream and run the custom function *labelImage* against it. The returned result will be an array of labels of the images recognized by the AI, containing labels ranked by confidence. Our rule takes the first of these labels with the highest confidence and sends it to the MQTT topic `ekuiper/labels`.

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

### Feed the data

Here we create a go program to send image data to the tfdemo topic to be processed by the rule. The model accepts an input of 224x224 pixels image. In the plugin, we have preprocessed the input image to resize it, so here we can feed any size of data to it. Some MQTT broker has a size limit for the payload, thus it is better to feed smaller image less than 2MB.

```go
package main

import (
  "fmt"
  "os"
  "time"

  mqtt "github.com/eclipse/paho.mqtt.golang"
)

func main() {
  const TOPIC = "tfdemo"

  images := []string{
    "peacock.png",
    "frog.jpg",
    // Other images
  }
  opts := mqtt.NewClientOptions().AddBroker("tcp://yourownhost:1883")
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

```

Run pub.go and it will start feeding images into the tfdemo theme. Please note that most MQTT brokers do not support transferring image files that are too large. In a practical scenario, we can adjust the size limit of the MQTT broker or use another source for the image stream input.

### Check the result

Since our rule definition has only one target: MQTT, the results will be written to the MQTT theme `ekuiper/labels`. Using an MQTT client subscribed to the theme, we input the tfdemo topic with two images *peacock.png* and *frog.png* and we will get two results.

```shell
{"label":"85:peacock"}
{"label":"33:tailed frog, bell toad, ribbed toad, tailed toad, Ascaphus trui"}
```

The images are correctly labeled.

## Conclusion

In this tutorial, we guide you through building custom eKuiper Python plugins that implement a label feature for real-time image streams using pre-trained TensorFlow Lite models. Developers can replace the models with their own desired models to implement their own plug-ins.
