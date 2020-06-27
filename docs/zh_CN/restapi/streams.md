# 流管理

Kuiper REST api 可用于管理流，例如创建、描述、显示和删除流定义。

## 创建流

该API用于创建流。 有关流定义的更多详细信息，请参考[流](../sqls/streams.md)。

```shell
POST http://localhost:9081/streams
```
请求样例，请求命令是带有`sql`字段的json字符串。

```json
{"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

该API可以运行任何流sql语句，而不仅可以创建流。

## 显示流

该API用于显示服务器中定义的所有流。

```shell
GET http://localhost:9081/streams
```

响应示例：

```json
["mystream"]
```

## 描述流

该API用于打印流的详细定义。

```shell
GET http://localhost:9081/streams/{id}}
```

响应示例：

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

## 删除流

该API用于删除流定义。

```shell
DELETE http://localhost:9081/streams/{id}
```