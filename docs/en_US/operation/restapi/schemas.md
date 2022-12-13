The eKuiper REST api for schemas allows you to manage schemas, such as create, show, drop and describe schemas.

## Create a schema

The API accepts a JSON content and create a schema. Each schema type has a standalone endpoint. Currently, only one schema type `protobuf` is supported. Schema is identified by its name, so the name must be unique for each type.

```shell
POST http://localhost:9081/schemas/protobuf
```

Schema content inside request body: 

```json
{
  "name": "schema1",
  "content": "message Book {required string title = 1; required int32 price = 2;}"
}
```

Schema content in a file：

```json
{
  "name": "schema2",
  "file": "file:///tmp/ekuiper/internal/schema/test/test2.proto"
}
```

Schema with static plugin：

```json
{
  "name": "schema2",
  "file": "file:///tmp/ekuiper/internal/schema/test/test2.proto",
   "soFile": "file:///tmp/ekuiper/internal/schema/test/so.proto"
}
```


### Parameters

1. name：the unique name of the schema.
2. schema content, use `file` or `content` parameter to specify. After schema created, the schema content will be written into file `data/schemas/$shcema_type/$schema_name`.
   - file: the url of the schema file. The url can be `http` or `https` scheme or `file` scheme to refer to a local file path of the eKuiper server. The schema file must be the file type of the corresponding schema type. For example, protobuf schema file's extension name must be .proto.
   - content: the text content of the schema.
3. soFile：The so file of the static plugin. Detail about the plugin creation, please check [customize format](../../rules/codecs.md#format-extension).

## Show schemas

The API is used for displaying all schemas defined in the server.

```shell
GET http://localhost:9081/schemas/protobuf
```

Response Sample:

```json
["schema1","schema2"]
```

## Describe a schema

The API is used for print the detailed definition of a schema.

```shell
GET http://localhost:9081/schemas/protobuf/{name}
```

Path parameter `name` is name of the schema.

Response Sample:

```json
{
  "type": "protobuf",
  "name": "schema1",
  "content": "message Book {required string title = 1; required int32 price = 2;}",
  "file": "ekuiper\\etc\\schemas\\protobuf\\schema1.proto"
}
```

## Delete a schema

The API is used for dropping the schema.

```shell
DELETE http://localhost:9081/schemas/protobuf/{name}
```

## Update a schema

The API is used for updating the schema. The request body is the same as creating a schema.

```shell
PUT http://localhost:9081/schemas/protobuf/{name}

{
  "name": "schema2",
  "file": "http://ahot.com/test2.proto"
}
```