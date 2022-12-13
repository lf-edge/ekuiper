# Codecs

The eKuiper uses a map based data structure internally during computation, so source/sink connections to external systems usually require codecs to convert the format. In source/sink, you can specify the codec scheme to be used by configuring the parameters `format` and `schemaId`.

## Format

There are two types of formats for codecs: schema and schema-less formats. The formats currently supported by eKuiper are `json`, `binary`, `protobuf` and `custom`. Among them, `protobuf` is the schema format.
The schema format requires registering the schema first, and then setting the referenced schema along with the format. For example, when using mqtt sink, the format and schema can be configured as follows

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

All formats provide the ability to codec and, optionally, the definition of schema. The codec computation can be built-in, such as JSON parsing; dynamic parsing schema for codecs, such as Protobuf parsing `*.proto` files; or user-defined static plug-ins (`*.so`) can be used for parsing. Among them, static parsing has the best performance, but it requires writing additional code and compiling into a plugin, which is more difficult to change. Dynamic parsing is more flexible.

All currently supported formats, their supported codec methods and modes are shown in the following table.


| Format   | Codec        | Custom Codec           | Schema                 |
|----------|--------------|------------------------|------------------------|
| json     | Built-in     | Unsupported            | Unsupported            |
| binary   | Built-in     | Unsupported            | Unsupported            |
| protobuf | Built-in     | Supported              | Supported and required |
| custom   | Not Built-in | Supported and required | Supported and optional |

### Format Extension

When using `custom` format or `protobuf` format, the user can customize the codec and schema in the form of a go language plugin. Among them, `protobuf` only supports custom codecs, and the schema needs to be defined by `*.proto` file. The steps for customizing the format are as follows:

1. implement codec-related interfaces. The Encode function encodes the incoming data (currently always `map[string]interface{}`) into a byte array. The Decode function, on the other hand, decodes the byte array into `map[string]interface{}`. The decode function is called in source, while the encode function will be called in sink.
    ```go
    // Converter converts bytes & map or []map according to the schema
    type Converter interface {
        Encode(d interface{}) ([]byte, error)
        Decode(b []byte) (interface{}, error)
    }
    ```
2. Implements the schema description interface. If the custom format is strongly typed, then this interface can be implemented. The interface returns a JSON schema-like string for use by source. The returned data structure will be used as a physical schema to help eKuiper implement capabilities such as SQL validation and optimization during the parse and load phase.
    ```go
    type SchemaProvider interface {
	    GetSchemaJson() string
    }
    ```
3. Compile as a plugin so file. Usually, format extensions do not need to depend on the main eKuiper project. Due to the limitations of the Go language plugin system, the compilation of the plugin still needs to be done in the same compilation environment as the main eKuiper application, including the same operations, Go language version, etc. If you need to deploy to the official docker, you can use the corresponding docker image for compilation.
    ```shell
    go build -trimpath --buildmode=plugin -o data/test/myFormat.so internal/converter/custom/test/*.go
    ```
4. Register the schema by REST API.
    ```shell
    ###
    POST http://{{host}}/schemas/custom
    Content-Type: application/json
    
    {
      "name": "custom1",
       "soFile": "file:///tmp/custom1.so"
    }
    ```
5. Use custom format in source or sink with `format` and `schemaId` parameters.

The complete custom format can be found in [myFormat.go](https://github.com/lf-edge/ekuiper/blob/master/internal/converter/custom/test/myformat.go). This file defines a simple custom format where the codec actually only calls JSON for serialization. It returns a data structure that can be used to infer the data structure of the eKuiper source.

### Static Protobuf

使用 Protobuf 格式时，我们支持动态解析和静态解析两种方式。使用动态解析时，用户仅需要在注册模式时指定 proto 文件。在解析性能要求更高的条件下，用户可采用静态解析的方式。静态解析需要开发解析插件，其步骤如下：

1. Assume we have a proto file helloworld.proto. Use official protoc tool to generate go code. Check [Protocol Buffer Doc](https://developers.google.com/protocol-buffers/docs/reference/go-generated) for detail.
   ```shell
   protoc --go_opt=Mhelloworld.proto=com.main --go_out=. helloworld.proto
   ```
2. Move the generated code helloworld.pb.go to the go language project and rename the package to main.
3. Create the wrapper struct for each message type. Implement 3 methods `Encode`, `Decode`, `GetXXX`. The main purpose of encoding and decoding is to convert the struct and map types of messages. Note that to ensure performance, do not use reflection. 
4. Compile as a plugin so file. Usually, format extensions do not need to depend on the main eKuiper project. Due to the limitations of the Go language plugin system, the compilation of the plugin still needs to be done in the same compilation environment as the main eKuiper application, including the same operations, Go language version, etc. If you need to deploy to the official docker, you can use the corresponding docker image for compilation.
   ```shell
    go build -trimpath --buildmode=plugin -o data/test/helloworld.so internal/converter/protobuf/test/*.go
   ```
5. Register the schema by REST API. Notice that, the proto file and the so file are needed.
    ```shell
    ###
    POST http://{{host}}/schemas/protobuf
    Content-Type: application/json
    
    {
      "name": "helloworld",
      "file": "file:///tmp/helloworld.proto",
       "soFile": "file:///tmp/helloworld.so"
    }
    ```
6. Use custom format in source or sink with `format` and `schemaId` parameters.

The complete static protobuf plugin can be found in [helloworld protobuf](https://github.com/lf-edge/ekuiper/tree/master/internal/converter/protobuf/test).


## Schema

A schema is a set of metadata that defines the data structure. For example, the .proto file is used in the Protobuf format as the data format for schema definition transfers. Currently, eKuiper supports schema types protobuf and custom.

### Schema Registry

Schemas are stored as files. The user can register the schema through the configuration file or the API. The schema is stored in `data/schemas/${type}`. For example, a schema file in protobuf format should be placed in `data/schemas/protobuf`.

When eKuiper starts, it will scan this configuration folder and automatically register the schemas inside. If you need to register or manage schemas on the fly, this can be done through the schema registry API, which acts on the file system.

### Schema Registry API

Users can use the schema registry API to add, delete, and check schemas at runtime. For more information, please refer to.

- [schema registry REST API](../operation/restapi/schemas.md)
- [schema registry CLI](../operation/cli/schemas.md)