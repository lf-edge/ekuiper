# Tables management

The eKuiper REST api for tables allows you to manage the tables, such as create, describe, show and drop table definitions.

## create a table

The API is used for creating a table. For more detailed information of table definition, please refer to [tables](../../sqls/tables.md).

```shell
POST http://localhost:9081/tables
```
Request sample, the request is a json string with `sql` field.

```json
{"sql":"create table my_table (id bigint, name string, score float) WITH ( datasource = \"lookup.json\", FORMAT = \"json\", KEY = \"id\")"}
```

This API can run any table sql statements, not only table creation.

## show tables

The API is used for displaying all of tables defined in the server.

```shell
GET http://localhost:9081/tables
```

Response Sample:

```json
["mytable"]
```

This API accepts one parameter kind, the value could be `scan` or `lookup` to query each kind of tables. Other values are invalid, it will return all kinds of tables. In below example, we can query all the lookup tables.

```shell
GET http://localhost:9081/tables?kind=lookup
```

## describe a table

The API is used for print the detailed definition of table.

```shell
GET http://localhost:9081/tables/{id}}
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
    "DATASOURCE": "lookup.json",
    "FORMAT": "JSON"
  }
}
```

## update a table

The API is used for update the table definition.

```shell
PUT http://localhost:9081/tables/{id}
```

Path parameter `id` is the id or name of the old table.

Request sample, the request is a json string with `sql` field.

```json
{"sql":"create table my_table (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

## drop a table

The API is used for drop the table definition.

```shell
DELETE http://localhost:9081/tables/{id}
```

