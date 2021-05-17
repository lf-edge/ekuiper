# 外部函数

外部函数通过配置的方式，将已有的服务映射成 Kuiper SQL 函数。运行使用外部函数的规则时，Kuiper 将根据配置，对数据输入输出进行转换，并调用对应的服务。

## 配置

外部函数的配置文件为 json 格式。通常包括两个部分：
- json 文件，用于描述服务的信息。文件将保存为 Kuiper 中服务的名字。
- schema 文件，用于描述服务 API 接口。包括服务包含的 API 名字，输入输出参数类型等。目前仅支持 [protobuf 类型](https://developers.google.com/protocol-buffers) 。

json 配置文件包括以下两个部分：

- about: 用于描述服务的元信息，包括作者，详细描述，帮助文档 url 等。详细用法请参考下面的范例。
- interfaces: 用于定义一组服务接口。同一个服务器提供的服务往往具有相同的服务地址，可作为一个服务接口。每一个服务接口包含下列属性：
    - protocol: 服务采用的协议。目前支持 "grpc", "rest" 和 "msgpack-rpc"
    - adddress: 服务地址，必须为 url。例如，典型 rpc 服务地址："tcp://localhost:50000" 或者 http 服务地址 "https://localhost:8000"。
    - schemaType: 服务描述文件类型。目前仅支持 "protobuf"。
    - schemaFile: 服务描述文件，目前仅支持 proto 文件。rest 和 msgpack 服务也需要采用 proto 描述。
    - functions: 函数映射数组，用于将 schema 里定义的服务映射到 SQL 函数。主要用于提供函数别名，例如 `{"name":"helloFromMsgpack","serviceName":"SayHello"}` 将服务定义中的 SayHello 服务映射为 SQL 函数 helloFromMsgpack 。未做映射的函数，其定义的服务以原名作为 SQL 函数名。
    - options: 服务接口选项。不同的服务类型有不同的选项。其中， rest 服务可配置的选项包括：
        - headers: 配置 http 头
        - insecureSkipVerify: 是否跳过 https 安全检查
    
假设我们有服务名为 'sample'，则可定义其名为 sample.json 的服务定义文件如下：

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

该文件定义了 sample 服务，其中包含 3 个服务接口的调用信息:
- trueno: grpc 服务
- tsrest: rest 服务
- tsrpc：msgpack-rpc 服务

每个服务接口提供的服务由其对应的 schema 文件定义。以 tsrest 为例，其 schema 文件为 tsrest.proto，定义如下：

```protobuf
syntax = "proto3";
package ts;

service TSRest { // proto service 名字与 Kuiper 外部服务名字无关
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

该文件定义了 tsrest 服务接口提供了一个服务 object_detection, 其输入，输出的格式也通过 protobuf 的格式进行了定义。建议 proto 文件中仅定义一个 `service`，可包含多个 `rpc`。

Protobuf 采用 proto3 格式，详细格式请参考 [proto3-spec](https://developers.google.com/protocol-buffers/docs/reference/proto3-spec) 。

### 映射

外部服务配置需要1个 json 文件和至少一个 schema（.proto） 文件。配置定义了服务映射的3个层次。

1. Kuiper 外部服务层: 外部服务名通过 json 文件名定义。这个名字将作为 [REST API](../restapi/services.md) 中描述，删除和更新整体外部服务的键。
2. 接口层: 定义于 json 文件的 `interfaces` 部分。该层为用户不可见的虚拟层，主要用于将一组服务聚合，以便可以只定义一次一组函数共有的属性，例如 schema，访问地址等。 
3. Kuiper 函数层: 函数定义于 proto 文件中的`rpc`。需要注意的是，proto 文件中的 `rpc` 必须定义在 proto 文件中的 `service` 之下。此 `sevice` 与 Kuiper 中的外部服务概念不同，且没有关联，其取名没有任何限制。默认情况下，外部函数的名字与 rpc 名字相同。用户可通过修改 json 文件中，interface 下的 functions 部分来覆盖函数名的映射关系。 

在这个样例中，如果用户在 Kuiper SQL 中调用 `objectDetection` 函数，则其映射过程如下:

1. 在 json 文件的 *tsrest* interface 中，找到函数映射：`{"name": "objectDetect","serviceName": "object_detection"}`。 该配置将 SQL 函数 `objectDetect` 映射为名为`object_detection` 的 rpc。
2. 在 `tsrest.proto` 文件中，找到 rpc `object_detection` 定义。再根据 json 文件中的 `tsrest` interface 配置属性，例如地址，协议等在运行时进行参数解析和服务调用。

需要注意的是，REST 服务调用时参数将会解析为 json。其中，json 的键名来自于 proto 中的 message 定义的键名。Proto message 的键名在解析时会自动转化为小写驼峰格式。如果调用的 REST 服务参数不是这种格式，用户必须在 message 中指定 json_name 选项显式指定键名以防止自动转换。

### 限制

由于 REST 和 msgpack-rpc 并非原生采用 protobuf 定义，因此其使用有一些限制。

REST 服务目前默认为 **POST**，且传输格式为 json。定义的protobuf 中：
- 输入参数仅可以为 message 类型或者 *google.protobuf.StringValue* 类型。若输入参数为 *google.protobuf.StringValue*，则传入的参数必须为已编码的 json 字符串，例如 `"{\"name\":\"name1\",\"size\":1}"`。

msgpack-rpc 服务有以下限制：
- 输入不能为空

## 注册和管理

外部函数需要注册后才能使用。其注册方法有两种：
- 放置在配置文件夹
- 通过 REST API 动态注册。

Kuiper 启动时，会读取配置文件夹 *etc/services* 里的外部服务配置文件并注册。用户可在启动之前，将配置文件遵循如下规则放入配置文件夹：
1. 文件名必须为 *$服务名$.json*。例如，*sample.json* 会注册为 sample 服务。
2. 使用的 Schema 文件必须放入 schema 文件夹。其目录结构类似为:
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
注意：Kuiper 启动之后，修改配置文件**不能**自动载入系统。需要动态更新时，请使用 REST 服务。

服务的动态注册和管理，请参考[外部服务管理 API](../restapi/services.md)。

## 使用

服务注册之后，其中定义的所有函数都可以在规则中使用。以上文 sample.json 中定义的 rest 服务函数 object_detection 为例，在 functions 中，映射为 objectDetection 函数。因此，调用该函数的 SQL 为：

```SQL
SELECT objectDetection(cmd, img) from comandStream
```

调用前，需要确保 REST 服务运行于 *http://localhost:8090* 且其中有 API *http://localhost:8090/object_detection* 。

### 参数展开

ptoto 文件中，一般参数为 message 类型。映射到 Kuiper 中，其参数可接收两种情况：

1. 参数不展开，传入的必须为 struct 类型
2. 参数展开，按照 message 中定义的顺序，传入多个参数

在上面的例子中，objectDetection 接收一个 message 参数。
```protobuf
message ObjectDetectionRequest {
  string cmd = 1;
  string base64_img = 2 [json_name="base64_img"];
}
```

在 Kuiper 中，用户可传入整个 struct 作为参数，也可以传入两个 string 参数，分别作为 cmd 和 base64_img。