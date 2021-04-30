# External Function

External functions map existing services to Kuiper SQL functions through configuration. When running the rules that use external functions, Kuiper will convert data input and output according to the configuration, and call the corresponding service.

## Configuration

The configuration file of the external function is in json format, which usually consists of two parts:

- JSON file, used to describes the information of the service. The file will be saved as the name of the service in Kuiper.
- Schema file, used to  describes the service API interface, including the name of the API included in the service,, input and output parameter type. Currently only [protobuf type](https://developers.google.com/protocol-buffers) is supported.

The json configuration file includes the following two parts:

- about: Used to describe the Meta-information of service, including author, detailed description, help document url, etc. For detailed usage, please refer to the example below.
- interfaces: Used to define a set of service interfaces. Services provided by the same server often have the same service address and can be used as a service interface. Each service interface contains the following attributes:
    - protocol: The protocol used by the service. "grpc", "rest" and "msgpack-rpc" are supported currently.
    - address: Service address, which must be url. For example, typical rpc service address: "tcp://localhost:50000" or http service address "https://localhost:8000".
    - schemaType: The type of service description file. Only "protobuf" is supported currently .
    - schemaFile: service description file, currently only proto file is supported. The rest and msgpack services also need to be described in proto.
    - functions: function mapping array, used to map the services defined in the schema to SQL functions. It is mainly used to provide function aliases. For example,`{"name":"helloFromMsgpack","serviceName":"SayHello"}` can map the SayHello service in the service definition to the SQL function helloFromMsgpack. For unmapped functions, the defined service uses the original name as the SQL function name.
    - options: Service interface options. Different service types have different options. Among them, the configurable options of rest service include:
      - headers: configure HTTP headers
      - insecureSkipVerify: whether to skip the HTTPS security check

Assuming we have a service named 'sample', we can define a service definition file named sample.json as follows:

```json
{
  "about": {
    "author": {
      "name": "EMQ",
      "email": "contact@emqx.io",
      "company": "EMQ Technologies Co., Ltd",
      "website": "https://www.emqx.io"
    },
    "helpUrl": {
      "en_US": "https://github.com/emqx/kuiper/blob/master/docs/en_US/plugins/functions/functions.md",
      "zh_CN": "https://github.com/emqx/kuiper/blob/master/docs/zh_CN/plugins/functions/functions.md"
    },
    "description": {
      "en_US": "Sample external services for test only",
      "zh_CN": "示例外部函数配置，仅供测试"
    }
  },
  "interfaces": {
    "trueno": {
      "address": "tcp://localhost:50051",
      "protocol": "grpc",
      "schemaType": "protobuf",
      "schemaFile": "trueno.proto"
    },
    "tsrest": {
      "address": "http://localhost:8090",
      "protocol": "rest",
      "options": {
        "insecureSkipVerify": true,
        "headers": {
          "Accept-Charset": "utf-8"
        }
      },
      "schemaType": "protobuf",
      "schemaFile": "tsrest.proto",
      "functions": [
        {
          "name": "objectDetect",
          "serviceName": "object_detection"
        }
      ]
    },
    "tsrpc": {
      "address": "tcp://localhost:9000",
      "protocol": "msgpack-rpc",
      "schemaType": "protobuf",
      "schemaFile": "tsrpc.proto",
      "functions": [
        {
          "name": "getFeature",
          "serviceName": "get_feature"
        },
        {
          "name": "getSimilarity",
          "serviceName": "get_similarity"
        }
      ]
    }
  }
}
```

This file defines the sample service, which contains the call information of 3 service interfaces:

- trueno: grpc service
- tsrest: rest service
- tsrpc: msgpack-rpc service

The service provided by each service interface is defined by its corresponding schema file. Taking tsrest as an example, its schema file is tsrest.proto, which is defined as follows:

```protobuf
syntax = "proto3";
package ts;

service TSRest {
  rpc object_detection(ObjectDetectionRequest) returns(ObjectDetectionResponse) {}
}

message ObjectDetectionRequest {
  string cmd = 1;
  string base64_img = 2;
}

message ObjectDetectionResponse {
  string info = 1;
  int32 code = 2;
  string image = 3;
  string result = 4;
  string type = 5;
}
```

This file defines the tsrest service interface to provide a service object_detection, and its input and output formats are also defined by the protobuf format.

Protobuf uses proto3 format. Please refer to [proto3-spec](https://developers.google.com/protocol-buffers/docs/reference/proto3-spec) for detailed format.

### Limitation

Since REST and msgpack-rpc are not natively defined by protobuf, there are some  limitations when using them.

The REST service is **POST** by default currently, and the transmission format is json. In the defined protobuf:

- The input and output format cannot be a basic type, and it must be message

The msgpack-rpc service has the following limitation:
- Input can not be empty

## Registration and Management

External functions need to be registered before being used. There are two ways to register:

- Placed in the configuration folder
- Dynamic registration via REST API.

When Kuiper is started, it will read and register the external service configuration file in the configuration folder *etc/services*. Before starting, users can put the configuration file into the configuration folder according to the following rules:

1. The file name must be *$service name$.json*. For example, *sample.json* will be registered as a sample service.

2. The Schema file used must be placed in the schema folder. The directory structure is similar to:

   ```
   etc
     services
       schema
         sample.proto
         random.proto
         ...
       sample.json
       other.json
       ...
   ```
   Note: After Kuiper is started, it **cannot** automatically load the system by modifying the configuration file. If you need to update dynamically, please use the REST service.

For dynamic registration and management of services, please refer to [External Service Management API](../restapi/services.md).

## Usage

After the service is registered, all functions defined in it can be used in rules. Taking the rest service function object_detection defined in sample.json above as an example, it is mapped to the objectDetection function in functions. Therefore, the SQL to call this function is:

```SQL
SELECT objectDetection(cmd, img) from comandStream
```

Before calling the function, you need to make sure that the REST service is running on *http://localhost:8090* and there is an API *http://localhost:8090/object_detection* in it.

### Parameter expansion

In the ptoto file, the general parameters are in message type. When being mapped to Kuiper, its parameters can be received in two situations:

1. If the parameters are not expanded, they must be in struct type when being passed in.
2. If the parameters are expanded, multiple parameters can be passed in according to the order defined in the message.

In the above example, objectDetection receives a message parameter.

```protobuf
message ObjectDetectionRequest {
  string cmd = 1;
  string base64_img = 2;
}
```

In Kuiper, users can pass in the entire struct as a parameter, or pass in two string parameters as cmd and base64_img respectively.