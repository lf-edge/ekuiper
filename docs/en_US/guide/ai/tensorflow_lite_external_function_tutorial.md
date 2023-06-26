# Run TensorFlow Lite model with eKuiper external function

[LF Edge eKuiper](https://www.lfedge.org/projects/ekuiper/) is an edge lightweight IoT data analytics / streaming
software which can be run at all kinds of resource constrained IoT devices.

[TensorFlow Lite](https://www.tensorflow.org/lite/guide) is a set of tools to help developers run TensorFlow models on
mobile, embedded, and IoT devices. It enables on-device machine learning inference with low latency and a small binary
size.

By integrating eKuiper and TensorFlow Lite, users can analyze the data in stream by AI with prebuilt TensorFlow models.
In this tutorial, we will walk you through building a eKuiper external function plugin to label pictures produced by an edge
device in stream by pre-trained image recognition TensorFlow model. By using the external functions, eKuiper and external functions
can run in totally different processes or host machines, which means eKuiper and external functions can have different lifecycles, what's more, external functions
can provide services to others except eKuiper.

## Prerequisite

The external functions plugins will be a gRPC Server, so users should have knowledge of gRPC. This tutorial will give the example code to set up the GRPC server.
Users can download the example code [here](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/pythonGRPC.zip).

Users also need have basic knowledge of Docker.

## Develop the external function

In the example code, the gRPC Server provide ``label`` method, and users just need write an interface description file and register them into eKuiper. Then eKuiper can call the RPC method
just as built-in functions. The ``label`` method is powered by ``tflite_runtime`` image classification, for more detail, please check the `label.py` file in the example code.

This is the proto file for the external functions plugins that provide services. The parameter of ``label`` method should be base64 encoded image.

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

## Build and run the gRPC Server

We provide Dockerfile to build the gRPC server, go to the root path of [example code](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/pythonGRPC.zip) pythonGRPC.zip, run the following command to build the gRPC Server docker image

```shell
 docker build  -t test:1.1.1 -f deploy/Dockerfile-slim-python .
```

And then set up the service by following command

```shell
 docker run -d  -p 50051:50051 --name rpc-test test:1.1.1
```

Now, the gRPC server are providing services on 50051 port.

## Package and register the external function

### Package

Package a json description file and a proto file for the services in gRPC server by zip. The file structure inside the zip file should be like:
For more detail about the file format and content, please refer to [this](../../extension/external/external_func.md).

- schemas
  - sample.proto
- sample.json

You can get the example zip file in [example code](https://github.com/lf-edge/ekuiper/blob/master/docs/resources/pythonGRPC.zip) in ``ekuiper_package`` folder

### Register the external function

put the sample.zip file in /tmp directory in the same machine with eKuiper and register by cli

```shell
# bin/kuiper create service sample '{"name": "sample","file": "file:///tmp/sample.zip"}'
```

## Run the external function

Once the external function registered, we can use it in our rule. We will create a rule to receive base64 encoded image data from a mqtt topic and label the image by tflite model.

### Create the stream

Define the stream by eKuiper Cli. We create a mqtt stream named demo, it subscribe to topic ``tfdemo``.

```shell
#/bin/kuiper create stream demo '() with (DATASOURCE="tfdemo")'
```

### Create the rule

Define the rule by eKuiper cli.  We will create a select query. We just read the base64 encoded images from demo stream and run the custom function ``label`` against it. The result will be the label of the image recognized by the AI.

```shell
#/bin/kuiper query

Connecting to 127.0.0.1:20498... 
kuiper >  select label(image) from demo

```

### Feed the data

User need send the data in json format like this

```json
{"image": "base64 encoded data"}
```

User can get the real data from the example code in ``images/example.json`` file, just send it to the MQTT broker by a MQTT client

### Check the result

You can get the result after you publish the base64 encoded image.

```shell
kuiper > [{"label":{"results":[{"confidence":0.5789139866828918,"label":"tailed frog"},{"confidence":0.3095814287662506,"label":"bullfrog"},{"confidence":0.040725912898778915,"label":"whiptail"},{"confidence":0.03226377069950104,"label":"frilled lizard"},{"confidence":0.01566782221198082,"label":"agama"}]}}]
```

## Conclusion

In this tutorial, we walk you through building external function to leverage a pre-trained TensorFlowLite model. If you need to use other gRPC services, just follow the steps to create customized function. Enjoy the AI in edge device.
