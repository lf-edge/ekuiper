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
   - file: the url of the schema file. The url can be `http` or `https` scheme or `file` scheme to refer to a local file
   path of the eKuiper server. The referenced file can be either a single schema file or a zip archive.
     - Single schema file: The file extension must match the corresponding schema type. For example: .proto for protobuf schema.
     - Zip archive: The zip file must contain a single primary schema file at its root. Optionally, the zip file can also contain a folder with the same name as the schema (without the extension) to hold supporting files. Any other files or folders within the zip archive will be ignored. Example: For a schema named test, the test.zip file should have the following structure:

     ```text
       test.zip/
       ├── test.proto  (Primary schema file)
       └── test/          (Optional folder for supporting files)
         ├── helper.proto
         └── config.json
     ```

     - content: the text content of the schema.
3. soFile：The so file of the static plugin. Detail about the plugin creation, please check [customize format](../../guide/serialization/serialization.md#format-extension).

## Upload a schema file

Use multipart form data to create or update a schema from a local file. The uploaded file has the same semantics as the `file` property of the JSON API: its type and content are handled by the schema implementation. For example, protobuf accepts a single schema file or a zip archive with supporting files.

```shell
PUT http://localhost:9081/schemas/{type}/{name}/upload
```

```shell
curl -X PUT http://localhost:9081/schemas/protobuf/schema1/upload \
  -F "file=@/path/to/schema1.proto"
```

The request has no JSON body. The schema type and name come from the URL. The multipart body contains one required `file` field and an optional `version` field.

If the schema does not exist, the API creates it and returns `201 Created`. If it already exists, the API replaces it and returns `200 OK`. The temporary uploaded file is removed after the request completes.

Response example:

```json
{
  "type": "protobuf",
  "name": "schema1"
}
```

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

## Versioning

The schema can have an optional **version** field. This field is essential for controlling updates and ensuring that new
schemas are correctly applied. When you update a schema, the system compares the new version string to the existing one.
An update is only accepted if the new version is **lexically greater** than the old one. This comparison is a
character-by-character string comparison, not a numerical one. The control logic for all versioned APIs is the same;
please refer to the [Versioning Logic](../../guide/rules/overview.md#versioning-logic) for details.

Below is an example schema request with version:

```json
{
  "name": "schema2",
  "file": "file:///tmp/ekuiper/internal/schema/test/test2.proto",
  "version": "1756436910"
}
```
