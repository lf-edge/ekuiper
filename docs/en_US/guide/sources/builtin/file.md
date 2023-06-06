## File source

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper provides built-in support for reading file content into the eKuiper processing pipeline. The file source is
usually used as a [table](../../../sqls/tables.md), and it is the default type for create table statement. File sources
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
  # Read the files in a directory in parallel or not
  parallel: false
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
  # Decompress the file with the specified compression method. Support `gzip`, `zstd` method now.                                                                                                                                                                                                                                           |
  decompression: ""
```

### File Types

The file source supports monitoring files or folders. If the monitored location is a folder, all files in the folder are required to be of the same type. When monitoring a folder, it will read in files order by file name alphabetically.

The supported file types are

- json: standard JSON array format files,
  see [example](https://github.com/lf-edge/ekuiper/tree/master/internal/topo/source/test/test.json). If the file format is a line-separated JSON string, it needs to be defined in lines format.
- csv: comma-separated csv files are supported, as well as custom separators.
- lines: line-separated file. The decoding method of each line can be defined by the format parameter in the stream definition. For example, for a line-separated JSON string, the file type is set to lines and the format is set to json.

Some files may have most of the data in standard format, but have some metadata in the opening and closing lines of the file. The user can use the `ignoreStartLines` and `ignoreEndLines` arguments to remove the non-standard parts of the beginning and end so that the above file types can be parsed.

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

In the stream definition, set the stream data to be in `JSON` format.

```SQL
create stream linesFileDemo () WITH (FORMAT="JSON", TYPE="file", CONF_KEY="jsonlines"
```

Moreover, the lines file type can be combined with any format. For example, if you set the format to protobuf and
configure the schema, it can be used to parse data that contains multiple Protobuf encoded lines.