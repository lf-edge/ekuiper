# Table specs

eKuiper streams is unbounded and immutable, any new data are appended in the current stream for processing.  **Table** is provided to represent the current state of the stream. It can be considered as a snapshot of the stream. Users can use table to retain a batch of data for processing.

Table is not allowed to use alone in eKuiper. It is only recommended to join with streams. When joining with stream, table will be updated continuously when new event coming. However, only events arriving on the stream side trigger downstream updates and produce join output.

## Syntax

Table supports almost the same syntax as streams. To create a table, run the below SQL:

```sql
CREATE TABLE   
    table_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

Table supports the same [data types](./streams.md#data-types) as stream. 

Table also supports all [the properties of the stream](./streams.md#language-definitions). Thus, all the source type are also supported in table. Many sources are not batched which have one event at any given time point, which means the table will always have only one event. An additional property `RETAIN_SIZE` to specify the size of the table snapshot so that the table can hold an arbitrary amount of history data.

## Usage scenarios

Typically, table will be joined with stream with or without a window. When joining with stream, table data won't affect the downstream updata, it is treated like a static referenced data although it may be updated internally.

### Lookup table

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

### Filter by history state

In some scenario, we may have an event stream for data and another event stream as the control information. 

```sql
CREATE TABLE stateTable (
		id BIGINT,
		triggered bool
	) WITH (DATASOURCE="myTopic", FORMAT="JSON", TYPE="mqtt");

SELECT * FROM demo LEFT JOIN stateTable on demo.id = stateTable.id  WHERE triggered=true
```

In this example, a table `stateTable` is created to record the trigger state from mqtt topic *myTopic*. In the rule, the data of `demo` stream is filtered with the current trigger state.