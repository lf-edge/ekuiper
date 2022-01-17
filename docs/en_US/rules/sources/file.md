## File source

eKuiper provides built-in support for reading file content into the eKuiper processing pipeline. The file source is usually used as a [table](../../sqls/tables.md) and it is the default type for create table statement.

```sql
create table table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="lookup.json", FORMAT="json", TYPE="file");
```

You can use [cli](../../operation/cli/tables.md) or [rest api](../../operation/restapi/tables.md) to manage the tables.

The configure file for the file source is in */etc/sources/file.yaml* in which the path to the file can be specified.

```yaml
default:
  fileType: json
  # The directory of the file relative to eKuiper root or an absolute path.
  # Do not include the file name here. The file name should be defined in the stream data source
  path: data
  # The interval between reading the files, time unit is ms. If only read once, set it to 0
  interval: 0
```

With this yaml file, the table will refer to the file *${eKuiper}/data/lookup.json* and read it in json format.