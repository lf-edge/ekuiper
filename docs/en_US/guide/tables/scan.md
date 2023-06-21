# Scan Table Scenarios

Typically, table will be joined with stream with or without a window. When joining with stream, table data won't affect the downstream data, it is treated like a static referenced data, although it may be updated internally.

## Enrich data

A typical usage for table is as a lookup table. Sample SQL will be like:

```sql
CREATE TABLE table1 (
    id BIGINT,
    name STRING
  ) WITH (DATASOURCE="lookup.json", FORMAT="JSON", TYPE="file");

SELECT * FROM demo INNER JOIN table1 on demo.id = table1.id
```

In this example, a table `table1` is created to read json data from file *lookup.json*. Then in the rule, `table1` is joined with the stream `demo` so that the stream can lookup the name from the id.

The content of *lookup.json* file should be an array of objects. Below is an example:

```json
[
  {
    "id": 1541152486013,
    "name": "name1"
  },
  {
    "id": 1541152487632,
    "name": "name2"
  },
  {
    "id": 1541152489252,
    "name": "name3"
  }
]
```

## Filter by history state

In some scenario, we may have an event stream for data and another event stream as the control information.

```sql
CREATE TABLE stateTable (
    id BIGINT,
    triggered bool
  ) WITH (DATASOURCE="myTopic", FORMAT="JSON", TYPE="mqtt");

SELECT * FROM demo LEFT JOIN stateTable on demo.id = stateTable.id  WHERE triggered=true
```

In this example, a table `stateTable` is created to record the trigger state from mqtt topic *myTopic*. In the rule, the data of `demo` stream is filtered with the current trigger state.
