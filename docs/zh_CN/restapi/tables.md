# 表管理

eKuiper 支持 REST api 方式管理表，例如创建、描述、显示和删除。

## 创建表

该 API 用于创建表。更详细的表定义信息请参考[tables](../sqls/tables.md)。
```shell
POST http://localhost:9081/tables
```

请求示例，请求是一个带有 `sql` 字段的 json 字符串。
```json
{"sql":"create table my_table (id bigint, name string, score float) WITH ( datasource = \"lookup.json\", FORMAT = \"json\", KEY = \"id\")"}
```
这个API可以运行任何表的sql语句，不仅仅是建表。

## 查看所有的表

此API 用于显示 eKuiper 中定义的所有表
```shell
GET http://localhost:9081/tables
```

返回示例:
```json
["mytable"]
```

## 查看表的详细信息

该 API 用于打印表的详细定义。
```shell
GET http://localhost:9081/tables/{id}}
```

返回示例:
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
    "DATASOURCE": "lookup.json",
    "FORMAT": "JSON"
  }
}
```

## 更新表

该 API 用于更新表的定义。
```shell
PUT http://localhost:9081/tables/{id}
```

路径参数 `id` 是旧表的 id 或名称。
请求示例，请求是一个带有 `sql` 字段的 json 字符串。
```json
{"sql":"create table my_table (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

## 删除表

该 API 用于删除表。
```shell
DELETE http://localhost:9081/tables/{id}
```

