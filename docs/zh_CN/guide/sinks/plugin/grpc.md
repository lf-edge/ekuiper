# gRPC Sink

该动作对每条结果调用 [gRPC](https://grpc.io/) 服务的一元方法。结果由动作的[格式](../../serialization/serialization.md#格式)编码为该方法的
protobuf 请求消息，调用的响应将被丢弃。

## 编译插件

该动作已内置于 eKuiper 的 full 版本中。如需作为插件使用，请在 eKuiper 源代码根目录下运行以下命令：

```shell
go build -trimpath --buildmode=plugin -o plugins/sinks/Grpc.so extensions/sinks/grpc/grpc.go
```

然后将 `plugins/sinks/Grpc.so` 复制到 eKuiper 安装目录的 `plugins/sinks` 文件夹中，并重启 eKuiper 服务器以激活插件。

## 属性

| 属性名称               | 是否可选 | 说明                                                                                                                         |
|--------------------|------|----------------------------------------------------------------------------------------------------------------------------|
| server             | 否    | gRPC 服务器的地址，格式为 `host:port`，例如 `127.0.0.1:50051`。                                                                         |
| method             | 否    | 要调用的一元方法的全名，格式为 `package.Service/Method`，例如 `sensor.Collector/Report`。允许以 `/` 开头。支持[动态属性](../overview.md#动态属性)。 |
| timeout            | 是    | 连接服务器以及每次调用的超时时间。可以是以毫秒为单位的整数，也可以是 `10s` 这样的时长字符串。默认值为 `5000`。                                                             |
| metadata           | 是    | 每次调用的[元数据](https://grpc.io/docs/guides/metadata/)，为字符串值的映射。值支持[动态属性](../overview.md#动态属性)。                                |
| insecureSkipVerify | 是    | 是否跳过服务器证书的验证。设置为 `true` 将启用 TLS。                                                                                                  |
| certificationPath  | 是    | TLS 客户端证书文件路径。设置该项将启用 TLS。                                                                                                 |
| privateKeyPath     | 是    | TLS 客户端私钥文件路径。设置该项将启用 TLS。                                                                                                 |
| rootCaPath         | 是    | 用于验证服务器的 CA 证书文件路径。设置该项将启用 TLS。                                                                                            |
| certificationRaw   | 是    | base64 编码的客户端证书。如果同时定义了 `certificationPath`，则优先使用后者。                                                                       |
| privateKeyRaw      | 是    | base64 编码的客户端私钥。如果同时定义了 `privateKeyPath`，则优先使用后者。                                                                          |
| rootCARaw          | 是    | base64 编码的 CA 证书。如果同时定义了 `rootCaPath`，则优先使用后者。                                                                            |

未设置任何 TLS 属性时，连接为明文。支持其他通用的 sink 属性，请参阅[公共属性](../overview.md#公共属性)。

出于安全考虑，默认会拦截对回环或私网地址（如 `127.0.0.1`）的连接。如需连接此类地址，请在 `etc/kuiper.yaml` 中设置
`basic.enablePrivateNet: true`。详见[全局配置](../../../configuration/global_configurations.md)。

您可以通过 API 预先检查服务器的连通性：[连通性检查](../../../api/restapi/connection.md#连通性检查)。

## 编码请求

该动作将每条结果编码后的字节作为请求消息发送，因此需要以下公共属性：

- `format`：必须为 `protobuf`，或者为 `custom` 并使用可编码 protobuf 消息的格式插件，例如[静态 Protobuf](../../serialization/serialization.md#静态-protobuf)。
- `schemaId`：请求消息类型，格式为 `模式名称.消息名称`。该模式必须先在[模式注册表](../../serialization/serialization.md#模式注册)中注册。
- `sendSingle`：必须为 `true`，以便将每一行编码为一条请求消息。不支持通过 `batchSize` 或 `lingerInterval` 进行批量发送。

protobuf 格式根据不带包名的消息名称查找消息。如果服务的 `.proto` 文件声明了 `package`，请注册一个只包含请求消息且不声明 `package`
的模式。包名不会改变消息的编码，只需要在 `method` 中使用。

## 投递保证

规则启动时客户端连接到服务器，连接断开后会自动重连。连接状态会在规则状态中报告。如果服务器不可用，规则会继续运行，在服务器恢复之前调用都会失败。

如果调用以 `UNAVAILABLE`、`DEADLINE_EXCEEDED`、`RESOURCE_EXHAUSTED` 或 `ABORTED` 状态失败，该错误为 IO
错误，因此如果配置了[缓存](../overview.md#缓存)或重发，结果将会被重新发送。其他错误，例如 `INVALID_ARGUMENT` 或
`UNIMPLEMENTED`，再次发送相同的请求也不会成功，因此不会按重发间隔重试。错误会在规则状态中报告。

## 使用示例

假设服务器实现了以下服务：

```protobuf
syntax = "proto3";

package sensor;

service Collector {
  rpc Report (Reading) returns (Ack) {}
}

message Reading {
  string deviceId = 1;
  double temperature = 2;
}

message Ack {
  bool ok = 1;
}
```

通过[模式注册表 API](../../../api/restapi/schemas.md) 将请求消息注册为模式 `sensor`：

```shell
POST http://{{host}}/schemas/protobuf
Content-Type: application/json

{
  "name": "sensor",
  "content": "syntax = \"proto3\";\nmessage Reading {\n  string deviceId = 1;\n  double temperature = 2;\n}\n"
}
```

然后对每一行调用 `Report` 方法：

```json
{
  "id": "grpcReport",
  "sql": "SELECT deviceId, temperature FROM demo WHERE temperature > 30",
  "actions": [
    {
      "grpc": {
        "server": "127.0.0.1:50051",
        "method": "sensor.Collector/Report",
        "metadata": {
          "authorization": "Bearer token"
        },
        "format": "protobuf",
        "schemaId": "sensor.Reading",
        "sendSingle": true
      }
    }
  ]
}
```
