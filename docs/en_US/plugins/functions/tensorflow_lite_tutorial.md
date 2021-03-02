# Run TensorFlow Lite model with Kuiper function plugin

[EMQ X Kuiper](https://docs.emqx.io/en/kuiper/latest/) is an edge lightweight IoT data analytics / streaming software which can be run at all kinds of resource constrained IoT devices.

[TensorFlow Lite](https://www.tensorflow.org/lite/guide) is a set of tools to help developers run TensorFlow models on mobile, embedded, and IoT devices. It enables on-device machine learning inference with low latency and a small binary size.

By integrating Kuiper and TensorFlow Lite, users can analyze the data in stream by AI with prebuilt TensorFlow models. In this tutorial, we will walk you through building a kuiper plugin to label pictures (binary data) produced by an edge device in stream by pre-trained image recognition TensorFlow model.

## Prerequisite

To run the TensorFlow lite interpreter, we need a trained model. We won't cover how to train and cover a model in this tutorial, you can check [tflite converter](https://www.tensorflow.org/lite/convert) for how to do that. We can either train a new model or pick one online. In this tutorial, we will use [label image](https://github.com/mattn/go-tflite/tree/master/_example/label_image) model from [mattn/go-tflite](https://github.com/mattn/go-tflite). This repo creates a golang binding for tflite C API. We will also use it to implement our plugin.

## Develop the plugin

To integrate Kuiper with TensorFlow lite, we will develop a customized Kuiper function plugin to be used by Kuiper rules. As an example, we will create `LabelImage` function whose input is a binary type data representing an image and the output is a string representing the label of the image. For example, `LabelImage(col)` will produce `"peacock"` if the input image has a peacock.

To develop the function plugin, we need to:

1. Create the plugin go file.  For example, in kuiper source code, create *plugins/functions/labelImage/labelImage.go* file.
2. Create a struct that implements [api.Function interface](../../../../xstream/api/stream.go). 
3. Export the struct.

The key part of the implementation is the *Exec* function. The pseudo code is like:

```go
func (f *labelImage) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
	
    //... do some initialization and validation
    
    // decode the input image
	img, _, err := image.Decode(bytes.NewReader(arg[0]))
	if err != nil {
		return err, false
	}
	var outerErr error
	f.once.Do(func() {		
		// Load labels, tflite model and initialize the tflite interpreter
	})

	// Run the interpreter against the input image
	
    // Return the label with the highest possibility
    return result, true
}
```

Another thing to notice is the export of plugin. The function is stateless, so we will only export one struct instance. All rules use this function will share one instance to avoid overhead of creating instances and loading model. The model and label path will be specified at the instantiation.

```go
var LabelImage = labelImage{
	modelPath: "labelImage/mobilenet_quant_v1_224.tflite",
	labelPath: "labelImage/labels.txt",
}
```

Check [this tutorial](../plugins_tutorial.md) for detail steps of creating Kuiper plugins.  Please refer to [labelImage.go](../../../../plugins/functions/labelImage/labelImage.go) for the full source code.

## Build and install the plugin

To use the plugin, we need to build it in the environment where Kuiper will run and then install it in Kuiper.

### Install by pre-built zip

If using Kuiper docker images with tags like 1.1.1 or 1.1.1-slim which are based on debian, we can install the pre-built labelImage plugin. For example, to install the plugin for Kuiper 1.1.2 in docker image emqx/kuiper:1.1.2-slim, the pre-built zip file locates in *https://www.emqx.io/downloads/kuiper-plugins/v1.1.2/debian/functions/labelImage_amd64.zip*. Run the rest command as below to install.

```shell
POST http://{{kuiperHost:kuiperRestPort}}/plugins/functions
Content-Type: application/json

{"name":"labelImage", "file": "https://www.emqx.io/downloads/kuiper-plugins/v1.1.2/debian/functions/labelImage_amd64.zip"}
```

### Manual build

If you don't run Kuiper by official Kuiper docker image, the pre-built labelImage plugin will not fit due to the limitation of golang plugin. You will need to built the plugin manually. There are 3 steps to create the plugin zip file manually:

1. Build the TensorFlowLite C API.
2. Build the labelImage plugin.
3. Package the plugin with install script.

#### Build the TensorFlowLite C API

There is a very simple [instruction](https://github.com/tensorflow/tensorflow/tree/v2.2.0-rc3/tensorflow/lite/c) from the tensorflow repo about build the C API. We will expand it in detail step by step in this section. Notice that, the plugin only test against TensorFlow v2.2.0-rc3, so we will build upon this version. Take **ubuntu** as an example, below are the build steps:

1. Install [Python 3](https://www.tensorflow.org/install/pip#1.-install-the-python-development-environment-on-your-system).

2. Copy [requirements.txt](requirements.txt) to your location. Install required python lib: `pip3 install -r requirements.txt`. The requirements are from `tensorflow/tensorflow/tools/pip_package/setup.py` of the corresponding TensorFlow version.

3. Install [Bazel](https://docs.bazel.build/versions/4.0.0/install-ubuntu.html) which is the build tool for TensorFlow.

4. Clone [tesorflow repo](https://github.com/tensorflow/tensorflow), switch to the required branch by `git checkout v2.2.0-rc3 -b mybranch`.

5. Build the target .so file, the output will be in ./bazel-bin. Copy the two so to tensorflow/lib folder.

   ```bash
   $ cd $tensorflowSrc
   $ bazel build --config monolithic -c opt //tensorflow/lite:libtensorflowlite.so
   $ bazel build --config monolithic -c opt //tensorflow/lite/c:libtensorflowlite_c.so
   $ mkdir lib
   $ cp bazel-bin/tensorflow/lite/libtensorflowlite.so lib
   $ cp bazel-bin/tensorflow/lite/c/libtensorflowlite_c.so lib
   ```
6. Install the so files.
   1. Update ldconfig file. `sudo vi /etc/ld.so.conf.d/tflite.conf`.
   2. Add the path `{{tensorflowPath}}/lib` to tflite.conf then save and exit.
   3. Run ldconfig: `sudo ldconfig`.
   4. Check installation result: `ldconfig -p | grep libtensorflow`. Make sure the two so files are listed.

#### Build the labelImage plugin

Make sure the Kuiper github repo has cloned. The plugin source file is in *plugins/functions/labelImage/labelImage.go*. Export the paths of the tensorflow repo and built libraries before build the plugin.

```shell
$ cd {{kuiperRepoPath}}
$ export CGO_CFLAGS=-I/root/tensorflow
$ export CGO_LDFLAGS=-L/root/tensorflow/lib
$ go build -trimpath --buildmode=plugin -o plugins/functions/LabelImage.so plugins/functions/labelImage/*.go
```

By these commands, the plugin is built into plugins/functions/LabelImage.so. For development purpose, you can restart Kuiper to load this plugin automatically and do testing. After testing complete, we should package it in a zip which is ready to use by Kuiper plugin installation API so that it can be used in another machine such as in production environment.

#### Package the plugin

Package all files and directories inside *plugins/functions/labelImage* into a zip file along with the built LabelImage.so. The file structure inside the zip file should be like:

- etc
    - labels.txt
    - mobilenet_quant_v1_224.tflite
- lib
    - libtensorflowlite.so
    - libtensorflowlite_c.so
- install.sh
- LabelImage.so
- tflite.conf

Install the packaged plugin to the target system like [Install by pre-built zip](#install-by-pre-built-zip).

## Run the plugin

Once the plugin installed, we can use it in our rule. We will create a rule to receive image byte data from a mqtt topic and label the image by tflite model.

### Define the stream

Define the stream by Kuiper rest API. We create a stream named tfdemo whose format is binary and the topic is tfdemo.

```shell
POST http://{{host}}/streams
Content-Type: application/json

{"sql":"CREATE STREAM tfdemo () WITH (DATASOURCE=\"tfdemo\", FORMAT=\"BINARY\")"}
```

### Define the rule

Define the rule by Kuiper rest API.  We will create a rule named ruleTf. We just read the images from tfdemo stream and run the custom function *labelImage* against it. The result will be the label of the image recognized by the AI.

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

### Feed the data

Here we create a go program to send image data to the tfdemo topic to be processed by the rule.

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
        // other images you want
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

Run pub.go, it will start to feed images into tfdemo topic.

### Check the result

Because our rule definition has only one sink: log so the result will be written into the log file. We feed the stream with two images *peacock.png* and *frog.png*. Check the log file, we would find:

```shell
time="2021-02-05 16:23:29" level=info msg="sink result for rule ruleTf: [{\"labelImage\":\"peacock\"}]" file="sinks/log_sink.go:16" rule=ruleTf
time="2021-02-05 16:23:30" level=info msg="sink result for rule ruleTf: [{\"labelImage\":\"bullfrog\"}]" file="sinks/log_sink.go:16" rule=ruleTf
```

The images are labeled correctly.

## Conclusion

In this tutorial, we walk you through building a customized Kuiper plugin to leverage a pre-trained TensorFlowLite model. If you need to use other models, just follow the steps to create another function. Notice that, the built TensorFlow C API can be shared among all functions if running in the same environment. Enjoy the AI in edge device.