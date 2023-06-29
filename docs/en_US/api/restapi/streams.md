# Streams management

The eKuiper REST api for streams allows you to manage the streams, such as create, describe, show and drop stream definitions.

## create a stream

The API is used for creating a stream. For more detailed information of stream definition, please refer to [streams](../../sqls/streams.md).

```shell
POST http://localhost:9081/streams
```

Request sample, the request is a json string with `sql` field.

```json
{"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

This API can run any stream sql statements, not only stream creation.

## show streams

The API is used for displaying all of streams defined in the server.

```shell
GET http://localhost:9081/streams
```

Response Sample:

```json
["mystream"]
```

## describe a stream

The API is used for print the detailed definition of stream.

```shell
GET http://localhost:9081/streams/{id}}
```

Response Sample:

```shell
{
  "Name": "demo",
  "StreamFields": [
    {
      "Name": "temperature",
      "FieldType": {
        "Type": 2
      }
    },
    {
      "Name": "ts",
      "FieldType": {
        "Type": 1
      }
    }
  ],
  "Options": {
    "DATASOURCE": "demo",
    "FORMAT": "JSON"
  }
}
```

## Get stream schema

The API is used to get the stream schema. The schema is inferred from the physical and logical schema definitions.

```shell
GET http://localhost:9081/streams/{id}/schema
```

The format is like Json schema:

```json
{
    "id": {
        "type": "bigint"
  },
    "name": {
        "type": "string"
  },
    "age": {
        "type": "bigint"
  },
    "hobbies": {
        "type": "struct",
        "properties": {
          "indoor": {
            "type": "array",
            "items": {
              "type": "string"
            }
          },
          "outdoor": {
            "type": "array",
            "items": {
              "type": "string"
            }
          }
        }
    }
}
```

## update a stream

The API is used for update the stream definition.

```shell
PUT http://localhost:9081/streams/{id}
```

Path parameter `id` is the id or name of the old stream.

Request sample, the request is a json string with `sql` field.

```json
{"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

## drop a stream

The API is used for drop the stream definition.

```shell
DELETE http://localhost:9081/streams/{id}
```
