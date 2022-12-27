## File source

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper provides built-in support for reading file content into the eKuiper processing pipeline. The file source is
usually used as a [table](../../../sqls/tables.md) and it is the default type for create table statement. File sources
are also supported as streams, where it is usually necessary to set the `interval` parameter to pull updates at regular
intervals.

```sql
create table table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="lookup.json", FORMAT="json", TYPE="file");
```

You can use [cli](../../../api/cli/tables.md) or [rest api](../../../api/restapi/tables.md) to manage the tables.

The configure file for the file source is in */etc/sources/file.yaml* in which the path to the file can be specified.

```yaml
default:
  # The type of the file, could be json, csv and lines
  fileType: json
  # The directory of the file relative to kuiper root or an absolute path.
  # Do not include the file name here. The file name should be defined in the stream data source
  path: data
  # The interval between reading the files, time unit is ms. If only read once, set it to 0
  interval: 0
  # The sending interval between each event in millisecond
  sendInterval: 0
  # After read
  # 0: keep the file
  # 1: delete the file
  # 2: move the file to moveTo
  actionAfterRead: 0
  # The path to move the file to after read, only valid when the actionAfterRead is 2
  moveTo: /tmp/kuiper/moved
  # If the first line is header
  hasHeader: false
  # Define the columns. If header is defined, this will be override
  # columns: [id, name]
  # How many lines to be ignored at the beginning. Notice that, empty line will be ignored and not be calculated.
  ignoreStartLines: 0
  # How many lines to be ignored in the end. Notice that, empty line will be ignored and not be calculated.
  ignoreEndLines: 0
```

With this yaml file, the table will refer to the file *${eKuiper}/data/lookup.json* and read it in json format.