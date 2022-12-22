# Stream Statements

SQL statements are defined to create and manage streams.

## Create Stream

`CREATE STREAM` defines a stream that connects to an external system to load data stream.

```sql
CREATE STREAM   
    stream_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

For the detail stream spec, please check [stream](../guide/streams/overview.md).

Example:

```SQL
CREATE STREAM my_stream ()
WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id")
```

## Describe Stream

A statement to get the stream definition.

```SQL
DESCRIBE STREAM stream_name
```

## Drop Stream

Delete a stream. Please make sure all the rules which refer to the stream are deleted.

```SQL
DROP STREAM stream_name
```

## Show Streams

Display all the streams defined.

```SQL
SHOW STREAMS
```
