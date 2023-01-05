## 文件源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper 提供了内置支持，可将文件内容读入 eKuiper 处理管道。 文件源通常用作[表格](../../../sqls/tables.md)， 并且采用 create
table 语句的默认类型。文件源也支持作为用作流，此时通常需要设置 `interval` 参数以定时拉取更新。

```sql
CREATE TABLE table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="lookup.json", FORMAT="json", TYPE="file");
```

您可以使用 [cli](../../../api/cli/tables.md) 或 [rest api](../../../api/restapi/tables.md) 来管理表。

文件源的配置文件是 */etc/sources/file.yaml* ，可以在其中指定文件的路径。

```yaml
default:
  # 文件的类型，支持 json， csv 和 lines
  fileType: json
  # 文件以 eKuiper 为根目录的目录或文件的绝对路径。
  # 请勿在此处包含文件名。文件名应在流数据源中定义
  path: data
  # 读取文件的时间间隔，单位为ms。 如果只读取一次，则将其设置为 0
  interval: 0
  # 读取后，两条数据发送的间隔时间
  sendInterval: 0
  # 文件读取后的操作
  # 0: 文件保持不变
  # 1: 删除文件
  # 2: 移动文件到 moveTo 定义的位置
  actionAfterRead: 0
  # 移动文件的位置, 仅用于 actionAfterRead 为 2 的情况
  moveTo: /tmp/kuiper/moved
  # 是否包含文件头，多用于 csv。若为 true，则第一行解析为文件头。
  hasHeader: false
  # 定义文件的列。如果定义了文件头，该选项将被覆盖。
  # columns: [id, name]
  # 忽略开头多少行的内容。
  ignoreStartLines: 0
  # 忽略结尾多少行的内容。最后的空行不计算在内。
  ignoreEndLines: 0
```

### File Types

The file source supports monitoring files or folders. If the monitored location is a folder, all files in the folder are
required to be of the same type. When monitoring a folder, it will read in files order by file name alphabetically.

The supported file types are

- json: standard JSON array format files,
  see [example](https://github.com/lf-edge/ekuiper/tree/master/internal/topo/source/test/test.json). If the file format
  is a line-separated JSON string, it needs to be defined in lines format.
- csv: comma-separated csv files are supported, as well as custom separators.
- lines: line-separated file. The decoding method of each line can be defined by the format parameter in the stream
  definition. For example, for a line-separated JSON string, the file type is set to lines and the format is set to
  json.

Some files may have most of the data in standard format, but have some metadata in the opening and closing lines of the
file. The user can use the `ignoreStartLines` and `ignoreEndLines` arguments to remove the non-standard parts of the
beginning and end so that the above file types can be parsed.

### Example

File sources involve the parsing of file contents and intersect with format-related definitions in data streams. We
describe with some examples how to combine file types and formats for parsing file sources.

#### Read a csv with a custom separator

The standard csv separator is a comma, but there are a large number of files that use the csv-like format with custom
separators. Some csv-like files have column names defined in the first line instead of data.

```csv
id name age
1 John 56
2 Jane 34
```

When the file is read, the configuration file is as follows, specifying that the file has a header.

```yaml
csv:
  fileType: csv
  hasHeader: true
```

In the stream definition, set the stream data to ``DELIMITED`` format, specifying the separator with the ``DELIMITER``
parameter.

```SQL
create
stream cscFileDemo () WITH (FORMAT="DELIMITED", DATASOURCE="abc.csv", TYPE="file", DELIMITER=" ", CONF_KEY="csv"
```

#### Read multi-line JSON data

With a standard JSON file, the entire file should be a JSON object or an array. In practice, we often need to parse
files that contain multiple JSON objects. These files are not actually JSON themselves, but are considered to be
multiple lines of JSON data, assuming that each JSON object is a single line.

```text
{"id": 1, "name": "John Doe"}
{"id": 2, "name": "Jane Doe"}
{"id": 3, "name": "John Smith"}
```

When reading this file, the configuration file is as follows, specifying the file type as lines.

```yaml
jsonlines:
  fileType: lines
```

In the stream definition, set the stream data to be in ``JSON`` format.

```SQL
create
stream linesFileDemo () WITH (FORMAT="JSON", TYPE="file", CONF_KEY="jsonlines"
```

Moreover, the lines file type can be combined with any format. For example, if you set the format to protobuf and
configure the schema, it can be used to parse data that contains multiple Protobuf encoded lines.