# 编解码

eKuiper 计算过程中使用的是基于 Map 的数据结构，因此 source/sink 连接外部系统的过程中，通常需要进行编解码以转换格式。在 source/sink 中，都可以通过配置参数 `format` 和 `schemaId` 来指定使用的编解码方案。

## 格式

编解码的格式分为两种：有模式和无模式的格式。当前 eKuiper 支持的格式有 `json`, `binary` 和 `protobuf`。其中，`protobuf` 为有模式的格式。
有模式的格式需要先注册模式，然后在设置格式的同时，设置引用的模式。例如，在使用 mqtt sink 时，可配置格式和模式：

```json
{
  "mqtt": {
    "server": "tcp://127.0.0.1:1883",
    "topic": "sample",
    "format": "protobuf",
    "schemaId": "proto1.Book"
  }
}
```

## 模式

模式是一套元数据，用于定义数据结构。例如，Protobuf 格式中使用 .proto 文件作为模式定义传输的数据格式。目前，eKuiper 仅支持 Protobuf 这一种模式。

### 模式注册

模式采用文件的形式存储。用户可以通过配置文件或者 API 进行模式的注册。模式的存放位置位于 `data/schemas/${type}`。例如，protobuf 格式的模式文件，应该放置于 `data/schemas/protobuf`。

eKuiper 启动时，将会扫描该配置文件夹并自动注册里面的模式。若需要在运行中注册或管理模式，可通过模式注册表 API 来完成。API 的操作会作用到文件系统中。

### 模式注册表 API

用户可使用模式注册表 API 在运行时对模式进行增删改查。详情请参考：

- [模式注册表 REST API](../operation/restapi/schemas.md)
- [模式注册表 CLI](../operation/cli/schemas.md)

