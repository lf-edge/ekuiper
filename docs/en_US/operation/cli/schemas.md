# Schema Registry

The eKuiper command line tools allows you to manage schemas, such as create, show, drop, describe schemas.

## Register schema

The command is used for creating a schema. The schema's definition is specified with JSON format

```shell
create schema $schema_type $schema_name $schema_json
```

Schema can be created by two ways.

- Specify the content inside the command.

Example：

```shell
# bin/kuiper create schema protobuf schema1 '{"name": "schema1","content": "message Book {required string title = 1; required int32 price = 2;}"}'
```

This command create a schema named `schema1`, the schema content is provided by the content field in the json.

- Specify the schema file path.

Example：

```shell
# bin/kuiper create schema protobuf schema1 '{"name": "schema1","file": "file:///tmp/aschema.proto"}'
```

This command creates a schema named `schema1` whose content is provided by `file` field in the json. The file will be copied into `etc/schemas/protobuf` and renamed to `schema1.proto.

### Parameters

1. schema_type：schema type，the only available type now is `protobuf`。
2. schema_name：The unique name of the schema which is also the name of the schema file.
3. schema_json：The json to define the schema. It must contain name and file or content field.


## Show schemas

The command is used for displaying all schemas defined in the server.

```shell
show schemas $schema_type
```

Example：

```shell
# bin/kuiper show schemas protobuf
schema1
schema2
```

## Describe a schema

The command prints the detailed definition of a schema.

```shell
describe schema $schema_type $schema_name
```

Example：

```shell
# bin/kuiper describe schema protobuf schema1
{
  "type": "protobuf",
  "name": "schema1",
  "content": "message Book {required string title = 1; required int32 price = 2;}",
  "file": "ekuiper\\etc\\schemas\\protobuf\\schema1.proto"
}

```

## Drop a schema

The command drops the schema. The loaded schema in rules can continue to use until rules restart.

```shell
drop schema $schema_type $schema_name
```

Example：

```shell
# bin/kuiper drop schema protobuf schema1
Schema schema1 is dropped.
```