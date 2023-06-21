# Table

eKuiper streams is unbounded and immutable, any new data are appended in the current stream for processing.  **Table** is provided to represent the current state of the stream. It can be considered as a snapshot of the stream. Users can use table to retain a batch of data for processing.

There are two kinds of table:

- Scan table: accumulates the data in memory. It is suitable for smaller dataset and the table content do NOT need to share between rules.
- Lookup table: refer to external table content. It is suitable for bigger dataset and share table content across rules.

## Syntax

Table supports almost the same syntax as streams. To create a table, run the below SQL:

```sql
CREATE TABLE   
    table_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

Table supports the same [data types](../streams/overview.md#schema-in-stream-definition) as stream.

Table also supports all [the properties of the stream](../streams/overview.md#stream-properties). Thus, all the source type are also supported in table. Many sources are not batched which have one event at any given time point, which means the table will always have only one event. An additional property `RETAIN_SIZE` to specify the size of the table snapshot so that the table can hold an arbitrary amount of history data.

### Lookup Table Syntax

The syntax is the same as creating a normal scan table, just need to specify kind property to be `lookup`. Below is an example to create a lookup data, which binds to redis database 0.

```sql
CREATE TABLE alertTable() WITH (DATASOURCE="0", TYPE="redis", KIND="lookup")
```

Currently, only `memory`, `redis` and `sql` source can be lookup table.

### Table properties

| Property name | Optional | Description                                                                                                                                                                      |
|---------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DATASOURCE    | false    | The value is determined by source type. The topic names list if it's a MQTT data source. Please refer to related document for other sources.                                     |
| FORMAT        | true     | The data format, currently the value can be "JSON", "PROTOBUF" and "BINARY". The default is "JSON". Check [Binary Stream](../streams/overview.md#binary-stream) for more detail. |
| SCHEMAID      | true     | The schema to be used when decoding the events. Currently, only use when format is PROTOBUF.                                                                                     |
| KEY           | true     | The primary key of the table. For example, for SQL source key specifies the primary key in the SQL table. It is not obeyed by all source types.                                  |
| TYPE          | true     | The source type. Each source type may support one kind or both kind of tables. Please refer to related documents.                                                                |
| CONF_KEY      | true     | If additional configuration items are requied to be configured, then specify the config key here. See [MQTT stream](../sources/builtin/mqtt.md) for more info.                   |
| KIND          | true     | The table kind, could be `scan` or `lookup`. If not specified, the default value is `scan`.                                                                                      |

## Usage scenarios

Table is a way to keep a large bunch of state for both scan and lookup type. Scan table keeps state in memory while lookup table keeps them externally and possibly persisted. Scan table is easier to set up while lookup table can easily connect to existed persisted states. Both types are suitable for stream batch integrated calculation.

Please check below links for some typical scenarios.

- [Scan table scenarios](scan.md)
- [Lookup table scenarios](lookup.md)
