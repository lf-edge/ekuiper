## File source

Kuiper provides built-in support for reading file content into the Kuiper processing pipeline. The file source is usually used as a [table](../../sqls/tables.md) and it is the default type for create table statement.

```sql
CREATE TABLE table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="lookup.json", FORMAT="json", TYPE="file");
```


The configure file for the file source is in */etc/sources/file.yaml* in which the path to the file can be specified.

```yaml
default:
  fileType: json
  # The directory of the file relative to kuiper root or an absolute path.
  # Do not include the file name here. The file name should be defined in the stream data source
  path: data
  # The interval between reading the files, time unit is ms. If only read once, set it to 0
  interval: 0
```

With this yaml file, the table will refer to the file *${kuiper}/data/lookup.json* and read it in json format.