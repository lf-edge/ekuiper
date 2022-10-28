# Codecs

The eKuiper uses a map based data structure internally during computation, so source/sink connections to external systems usually require codecs to convert the format. In source/sink, you can specify the codec scheme to be used by configuring the parameters `format` and `schemaId`.

## Format

There are two types of formats for codecs: schema and schema-less formats. The formats currently supported by eKuiper are `json`, `binary` and `protobuf`. Among them, `protobuf` is the schema format.
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

## Schema

A schema is a set of metadata that defines the data structure. For example, the .proto file is used in the Protobuf format as the data format for schema definition transfers. Currently, eKuiper supports only one schema type Protobuf.

### Schema Registry

Schemas are stored as files. The user can register the schema through the configuration file or the API. The schema is stored in `data/schemas/${type}`. For example, a schema file in protobuf format should be placed in `data/schemas/protobuf`.

When eKuiper starts, it will scan this configuration folder and automatically register the schemas inside. If you need to register or manage schemas on the fly, this can be done through the schema registry API, which acts on the file system.

### Schema Registry API

Users can use the schema registry API to add, delete, and check schemas at runtime. For more information, please refer to.

- [schema registry REST API](../operation/restapi/schemas.md)
- [schema registry CLI](../operation/cli/schemas.md)