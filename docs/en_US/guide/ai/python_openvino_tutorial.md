# Running AI Algorithms with Python Function Plugins

[LF Edge eKuiper](https://www.lfedge.org/projects/ekuiper/) is an edge lightweight IoT data analytics / streaming
software which can be run at all kinds of resource constrained IoT devices.

[OpenVINO](https://www.intel.com/content/www/us/en/developer/tools/openvino-toolkit/overview.html) is an open source toolkit that makes it easier to write once, deploy anywhere.
It converts and optimizes models trained using popular frameworks like TensorFlow*, PyTorch*, and Caffe*. Deploy across a mix of Intel hardware and environments, on-premise and on-device, in the browser, or in the cloud.

By integrating eKuiper and OpenVINO, users can analyze the data more easily.
In this tutorial, we will walk you through building an AI-enabled approach to segment defects from the surface powered by eKuiper and OpenVINO.
The completed plugin package can be downloaded [here](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/openvinoProject.zip), which also contains the full source code.

## Prerequisite

Before starting the tutorial, please prepare the following products or environments.

1. Install the Python 3.x environment.
2. Install the opencv-python, numpy and openvino packages via `pip install opencv-python==4.7.0.* openvino==2023.0.0 numpy==1.24.3`.

By default, the portable plugin for eKuiper will run with the `python` command. If your environment does not support the `python` command, please use the [configuration file](../../configuration/global_configurations.md#portable-plugin-configurations) to modify the Python command, such as `python3`.

If you are developing with Docker, you can use the `lfedge/ekuiper:<tag>-slim-python` version. This version includes both the eKuiper and python environments.

## Develop the Plugin

To integrate eKuiper with OpenVINO, we will develop a custom eKuiper function plugin to use with eKuiper rules. We will create the `inference` function whose input is base64 encoded image data and whose output is json dictionary data containing the number of segment defects 、the processed image data and the cost for the inference.

To develop the function plugin, we need to：

1. implement the business logic in Python and wrap it as an eKuiper function.
2. Package the relevant files according to the plugin format.

Create Python files that implement the extended interface (source, sink, or function). In this tutorial, we are developing a function plugin, so we need to implement the function extension interface.

- Writing Python segment defects functions
- Wrapping an existing function as an eKuiper function plugin

### Implement the business logic

Our target function needs to take the base64 encoded image data as an input parameter, perform image preprocessing, load the OpenVINO models, call the OpenVINO for inference, extract the inference result, and output it. We need to implement this function using Python in the same way as we would write a normal Python function.

The inference function will receive the base64 encoded image data and return the result.

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

The above code is only related to the business logic and can be tested without calling eKuiper's SDK. We just need to make sure that the input and output is of a type that can be converted to JSON format. For example, if the return value is a numpy array, it needs to be converted to a list type first. Developers can add main functions or unit tests to their business logic file or to another file for testing. For example, the following main function can be used to test the above business logic.
make sure the model.xml and model.bin file are in the models directory.

```python
# To test the logic
if __name__ == '__main__':
    file_name = "test.jpg"
    with open(file_name, "rb") as f:
        cwd = './'
        result = inference(base64.b64encode(f.read()))
        print(json.dumps(result))
```

This file uses the `test.jpg` image file as input, calls the inference function to test it, and converts the result to a json string and prints it. This allows us to see if the function works as expected.

### Plugin Implementation

Like native plugins, Python plugins need to implement the corresponding interfaces; Python plugins also support the Source, Sink and Function interfaces, [interface definition](../../extension/portable/python_sdk.md#development) is similar to the native plugins. Here, what we need to implement is the function interface.

Create the `inference.py` function to wrap the functions implemented in the previous section. Import the Function class from eKuiper's plugin SDK and create the corresponding implementation class. The validate function is used to validate the parameters; is_aggregate is used to define whether the function is an aggregate function. The key implementations are in the exec function. Here, we take the data in the eKuiper stream as an argument, call the logic implemented above, and return the result to eKuiper.

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

Once the code is implemented, we also need to add a description file for each function, which is placed in the functions directory, in this case we create the `defect.json` file. This file will help the eKuiper manager to automatically produce the UI associated with the plugin.

### Plugin Packaging

At this point, we have completed the development of the main functionality, and next we need to package these files into a plugin format. Plugin packaging requires several steps to be completed.

1. If the plugin has additional dependencies, you need to create the dependency installation script `install.sh`. When the plugin is installed, eKuiper will look for an installation script file `install.sh` in the plugin package and execute it if there is one. In this case, we create a `requirements.txt` file listing all the dependency packages. The installation of the dependencies is done in `install.sh` by calling `pip install -r $cur/requirements.txt`. For other plugins, you can reuse this script to update `requirements.txt` if you have no special requirements.
2. Create a Python entry file that exposes all the implemented interfaces. Because multiple extensions can be implemented in a single plugin, you need an entry file that defines the implementation classes for each extension. The content is a main function, which is the entry point for the plugin runtime. It calls the methods in the SDK to define the plugin, including the plugin name, and a list of keys for the source, sink, and function implemented in the plugin. Here only a function plugin named `inference` is implemented, and its corresponding implementation method is `inferenceIns`. The Python plug-in process is independent of the eKuiper main process.

    ```python
    if __name__ == '__main__':
        # Define the plugin
        c = PluginConfig("defect", {}, {},
                         {"inference": lambda: inferenceIns})
        # Start the plugin instance
        plugin.start(c)
    ```

3. Create a plugin description file in JSON format to define the metadata of the plugin. The file name must be the same as the plugin name, i.e. `defect.json`. The function names defined therein must correspond exactly to the entry file, and the contents of the file are as follows. Where, executable is used to define the name of the plugin's executable entry file.

    ```json
    {
      "version": "v1.0.0",
      "language": "python",
      "executable": "main.py",
      "sources": [
      ],
      "sinks": [
      ],
      "functions": [
        "inference"
      ]
    }
    ```

At this point we have completed the development of the plugin, next we just need to package all the files in the directory into a zip file. The zip file should have a file structure similar to:

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

## Plugin Installation

As the same with installing native plugins, we can also install Python plugins via the eKuiper manager UI or the REST API. To use the REST API, upload the zip file packaged above to the machine where eKuiper is located. Then use the following API to install it:

```text
### Install pyai plugin
POST http://{{host}}/plugins/functions
Content-Type: application/json

{"name":"defect", "file": "file:///tmp/defect.zip"}
```

The installation process requires an Internet connection to download dependencies, including `ffmpeg libsm6 libxext6`, which may take a long time depending on the network conditions.

## Run the plugin

Once the plugin installed, we can use it in our rule. We will create a rule to receive image byte data from a mqtt topic and do the inference for the image by OpenVINO model.

### Define the stream

Define the stream by eKuiper rest API. We create a stream named openvino_demo and the topic is openvino_demo.

```shell
POST http://{{host}}/streams
Content-Type: application/json

{"sql":"CREATE STREAM openvino_demo () WITH (DATASOURCE=\"openvino_demo\")"}
```

### Define the rule

Define the rule by eKuiper rest API.  We will create a rule named ruleOp. We just read the base64 encoded images from openvino_demo stream and run the custom function *inference* against it.  
It will send out the base64 encoded origin and processed image to topic *ekuiper/defect* when the segment defects numbers not be zero.

sends it to the MQTT topic `ekuiper/labels`.

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

### Feed the data

Here we create a python program to send image data to the openvino_demo topic to be processed by the rule.
Users can get the full code [here](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/openvinoProject.zip)

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

### Check the result

Users can subscribe to the *ekuiper/defect* to get the notification when the images have segment defect.

## Conclusion

In this tutorial, we guide you through building custom eKuiper Python plugins that do segment defects for real-time image streams using OpenVINO models. Developers can replace the models with their own desired models to implement their own plug-ins.
