# Running OpenVINO Algorithms with Python Function Plugin

[LF Edge eKuiper](https://www.lfedge.org/projects/ekuiper/) is an edge lightweight IoT data analytics/streaming
software that can run on all kinds of resource-constrained IoT devices.

[OpenVINO](https://www.intel.com/content/www/us/en/developer/tools/openvino-toolkit/overview.html) is an open source toolkit that makes it easier to write once and deploy anywhere.
It converts and optimizes models trained using popular frameworks like TensorFlow, PyTorch, and Caffe. Deploy across a mix of Intel hardware and environments, on-premise and on-device, in the browser, or in the cloud.
The example code and models for OpenVINO take reference from [Intel - Industrial Surface Defect Detection Reference Implementation](https://www.intel.com/content/www/us/en/developer/articles/reference-implementation/industrial-surface-defect-detection.html).

By combining eKuiper and OpenVINO, data analysis can become more accessible and efficient.
This tutorial will guide you through creating an AI-based system for defect detection on surfaces, utilizing the power of eKuiper and OpenVINO.
A complete plugin package, including full source code, can be downloaded from [eKuiper Resources page](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/openvinoProject.zip).

## Prerequisite

Before starting the tutorial, please prepare the following products or environments.

1. Install the Python 3.x environment. And set the `pythonBin` under the portable configuration of [eKuiper configuration file](../../configuration/global_configurations.md#portable-plugin-configurations) to match your Python command (e.g., 'python3' if applicable).
2. Install the opencv-python, numpy and openvino packages via `pip install opencv-python==4.7.0.* openvino==2023.0.0 numpy==1.24.3`.

For Docker users:

Use the <span v-pre>lfedge/ekuiper:{{tag}}-slim-python</span> Docker image which includes eKuiper and Python
environments. If this Docker image lacks the required Python packages, install them using the pip command mentioned
earlier, either by extending the Dockerfile or running the command inside the Docker container.

## Develop the Plugin

To integrate eKuiper with OpenVINO, we will develop a custom eKuiper function plugin to use with eKuiper rules. This plugin will include an inference function that takes base64 encoded image data as input and outputs a JSON dictionary. The output dictionary will contain information such as the number of segmented defects, processed image data, and the cost of the inference process.

To develop the function plugin, we need to：

1. Implement the business logic in Python and wrap it as an eKuiper function.
2. Package the relevant files according to the plugin format.

3. Create a Python file that implements the extended interface (source, sink, or function):
   - Writing Python segment defects functions
   - Wrapping an existing function as an eKuiper function plugin

### Implement the Business Logic

Our goal is to create a function that accepts base64 encoded image data, conducts image preprocessing, loads OpenVINO models, makes inference calls through OpenVINO, extracts inference results, and provides an output. This function will be implemented as a standard Python function.

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

Ensure the `model.xml` and `model.bin` files are in the `models` directory.

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

### Build the Plugin

Similar to native plugins, Python plugins need to implement the corresponding interfaces, including the Source, Sink, and Function interfaces. The [Interface definition](../../extension/portable/python_sdk.md#development) for Python plugins is similar to the native plugins.

Create the `inference.py` function to encapsulate the previously implemented functionalities. Import the Function class from eKuiper's plugin SDK and create the corresponding implementation class. The `validate` function is used to validate the parameters; `is_aggregate` is to define whether the function is an aggregate function. The core implementations are in the `exec` function. Here, we take the data in the eKuiper stream as an argument, call the previously implemented logic, and return the result to eKuiper.

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

Once the code is implemented, we also need to add a description file for each function, which is placed in the functions directory, in this case, we create the `defect.json` file.

### Plugin Packaging

At this point, we have completed the development of the main functionality, and next we need to package these files into a plugin format:

1. **Managing Plugin Dependencies**: If the plugin has additional dependencies, you need to create the dependency installation script `install.sh`. When the plugin is installed, eKuiper will search for this script within the plugin package and executes it if found. In this case, we create a `requirements.txt` file listing all the dependency packages. The installation of the dependencies is done in `install.sh` by calling `pip install -r $cur/requirements.txt`. For other plugins, you can reuse this script to update `requirements.txt` if you have no special requirements.
2. **Create a Python Entry File**: Because multiple extensions can be implemented in a single plugin, you need an entry file that defines the implementation classes for each extension. The entry file is a main function, which is the entry point for the plugin runtime. It calls the methods in the SDK to define the plugin, including the plugin name, and a list of keys for the implemented source, sink, and function. Here only a function plugin named `inference` is implemented, with `inferenceIns` as its corresponding implementation method. The Python plug-in process operates independently from the eKuiper main process.

   ```python
       if __name__ == '__main__':
       # Define the plugin
       c = PluginConfig("defect", {}, {},
           {"inference": lambda: inferenceIns})
       # Start the plugin instance
       plugin.start(c)
   ```

3. **Establish a Plugin Description File**: Create a plugin description file in JSON format to define the metadata of the plugin. The file name must match the plugin name, i.e. `defect.json`. The function names defined within the file must align precisely with those in the entry file. The `executable` field is used to define the name of the plugin's executable entry file.

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

## Install the Plugin

The same as installing native plugins, the Python plugins can be installed with REST API. To use the REST API, upload the zip file packaged above to the machine where eKuiper is located. Then use the following API to install it:

```text
### Install pyai plugin
POST http://{{host}}/plugins/functions
Content-Type: application/json

{"name":"defect", "file": "file:///tmp/defect.zip"}
```

The installation process requires an Internet connection to download dependencies, including `ffmpeg libsm6 libxext6`, which may take a long time depending on the network conditions.

## Start the Plugin

After installing the plugin, it can be incorporated into our rule. We'll create a rule that receives image byte data from an MQTT topic and performs inference on the image using the OpenVINO model.

### Define the Stream

Define the stream by eKuiper rest API. We create a stream named openvino_demo and the topic is openvino_demo.

```shell
POST http://{{host}}/streams
Content-Type: application/json

{"sql":"CREATE STREAM openvino_demo () WITH (DATASOURCE=\"openvino_demo\")"}
```

### Define the Rule

Define the rule by eKuiper rest API. This rule will read base64 encoded images from the `openvino_demo` stream and apply the custom function `inference` to it.
If the number of segmented defects is non-zero, it will dispatch the base64 encoded original and processed images to the `ekuiper/defect` topic.

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

### Feed the Data

Here we create a Python program to send image data to the `openvino_demo` topic and to be processed by the rule.
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

### Check the Result

Users can subscribe to the `ekuiper/defect` topic to get a notification when the images exhibit segmented defects.

## Conclusion

In this tutorial, we guide you through building custom eKuiper Python plugins that do defect segmentation for real-time image streams using OpenVINO models. Developers can replace the models with their own desired models to develop customized plugins.
