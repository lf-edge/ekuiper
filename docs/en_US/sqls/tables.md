# Table specs

Kuiper streams are infinite.  **Table** is provided to read data from a finite source like a file or a normal database table as a batch. The batch source is supposed to be small because it will reside in the memory. The typical scenario to use table is to treat it as a static lookup dictionary to join with the stream.

## Syntax

Table supports almost the same syntax as streams. To create a table, run the below SQL:

```sql
CREATE TABLE   
    table_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

Table supports the same [data types](./streams.md#data-types) as stream. Compared to stream, it has the following limitations:

1. Currently, the only and default supported type is "file", and the source plugin is not supported.
2. Format "binary" is not supported.

## File type

Currently, the only supported type for table is file. To create a table that will read lookup.json file is like:

```sql
CREATE TABLE table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="lookup.json", FORMAT="json");
```
The configure file for the file source is in */etc/sources/file.yaml* in which the path to the file can be specified.

```yaml
default:
  fileType: json
  # The directory of the file relative to kuiper root or an absolute path.
  # Do not include the file name here. The file name should be defined in the stream data source
  path: data
```

With this yaml file, the table will refer to the file *${kuiper}/data/lookup.json* and read it in json format.

## Lookup table

A typical usage for table is as a lookup table. Sample SQL will be like:

```sql
SELECT * FROM demo INNER JOIN table1 on demo.ts = table1.id
```

Only when joining with a table, the join statement can be run without a window.