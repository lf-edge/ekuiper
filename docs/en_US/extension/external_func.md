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

service TSRest { // The proto service name is indifferent
  rpc object_detection(ObjectDetectionRequest) returns(ObjectDetectionResponse) {}
}

message ObjectDetectionRequest {
  string cmd = 1;
  string base64_img = 2 [json_name="base64_img"];
}

message ObjectDetectionResponse {
  string info = 1;
  int32 code = 2;
  string image = 3;
  string result = 4;
  string type = 5;
}
```

This file defines the tsrest service interface to provide a service object_detection, and its input and output formats are also defined by the protobuf format. It is recommended to only have one service section in the proto file.

Protobuf uses proto3 format. Please refer to [proto3-spec](https://developers.google.com/protocol-buffers/docs/reference/proto3-spec) for detailed format.

### HTTP Options

In order to support detail configuration of the REST service, such as the http method, the url template, the params and the body, an additional mapping annotations based on grpc transcoding specification provided by *google.api.http* annotation. Users can specify a http rule for each rpc method to define the mapping of the rpc method to the http method, URL path, URL query parameters, and HTTP request body.

Below is a portion of the revised tsrest.proto file in which a http rule is added. The rule specifies the http method to be *post*, and the mapping url to */v1/computation/object_detection* to override the default url */object_detection*. It also specifies the body to be a wildcard which means the whole input parameter of *ObjectDetectionRequest* will be the body.

```protobuf
service TSRest {
  rpc object_detection(ObjectDetectionRequest) returns(ObjectDetectionResponse) {
    option (google.api.http) = {
      post: "/v1/computation/object_detection"
      body: "*"
    };
  }
}
```

If the object_detection rest service provides different url for different command, users can specify the url mapping with parameters as below. By this way, the input *ObjectDetectionRequest* parameter's *cmd* field is assigned to the url, and the *base64_img* field is processed as the body.

```protobuf
service TSRest {
  rpc object_detection(ObjectDetectionRequest) returns(ObjectDetectionResponse) {
    option (google.api.http) = {
      post: "/v1/computation/object_detection/{cmd}"
      body: "base64_img"
    };
  }
}
```

Another typical scenario is the REST services to search a list. The search parameters are usually appended to the url as the query parameters. 

```protobuf
service TSRest {
  rpc SearchMessage(MessageRequest) returns(Message) {
    option (google.api.http) = {
      get: "/v1/messages"
    };
  }
}

message MessageRequest {
  string author = 1;
  string title = 2;
}
```

In this example, there is no *body* specified thus all parameter fields are mapped to the query parameter. When calling `SearchMessage({"author":"Author","title":"Message1"})` in Kuiper SQL, it will be mapped to `GET /v1/messages?author=Author&title=Message1`.

For more detail about the mapping syntax for protobuf, please check [adding transcoding mapping](https://cloud.google.com/endpoints/docs/grpc/transcoding#adding_transcoding_mappings) and [httprule](https://cloud.google.com/endpoints/docs/grpc-service-config/reference/rpc/google.api#httprule).

#### Usage

To use the http options, the google api package must be imported in the proto file.

```protobuf
syntax = "proto3";

package yourpackage;

import "google/api/annotations.proto";
```

Thus, the google api proto files must be in the imported path. Kuiper already ship those proto files in `etc/services/schemas/google`. Users do not need to add this to the packaged customized service.

### Mapping

In the external service configuration, there are 1 json file and at least 1 schema file(.proto) to define the function mapping. This will define a 3 layer mappings.

1. Kuiper external service layer: it is defined by the file name of the json. It will be used as a key for the external service in the [REST API](../restapi/services.md) for the describe, delete and update of the service as a whole.
2. Interface layer: it is defined in the `interfaces` section of the json file. This is a virtual layer to group functions with the same schemas so that the shared properties such as address, schema file can be specified only once.
3. Kuiper function layer: it is defined in the proto file as `rpc`. Notice that, the proto rpcs must be defined under a service section in protobuf. There is no restriction for the name of proto service. The function name is the same as the rpc name in the proto by default. But the user can override the mapping name in the json files's interfaces -> functions section.

In this sample, if a user call `objectDetection` function in Kuiper SQL, the mapping steps are:

1. Found a function mapping in json file, interfaces *tsrest* functions section: `{"name": "objectDetect","serviceName": "object_detection"}`. This maps SQL function `objectDetect` to rpc named `object_detection`.
2. In the schema file `tsrest.proto`, rpc `object_detection` is defined and the parameter and return type will be parsed. The `tsrest` interface properties such as address, protocol will be used to issue the request in runtime.

Notice that, in REST call the parameters will be parsed to json.  Proto message field names are **converted** to lowerCamelCase and become JSON object keys. If the object keys of the REST API is not lowerCamelCase, the user must specify the json_name field option to avoid the conversion.

### Notification

Since REST and msgpack-rpc are not natively defined by protobuf, there are some  limitations when using them.

The REST service is **POST** by default currently, and the transmission format is json. The user can change the default method through [http options](#http-options) in the defined protobuf. There are some restricitons in rest service:

- If http options are not specified, the input type must be **Message** or *google.protobuf.StringValue*. If the type is *google.protobuf.StringValue*, the parameter must be an encoded json string like `"{\"name\":\"name1\",\"size\":1}"`.
- The marshalled json for int64 type will be string

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
  string base64_img = 2 [json_name="base64_img"];
}
```

In Kuiper, users can pass in the entire struct as a parameter, or pass in two string parameters as cmd and base64_img respectively.

