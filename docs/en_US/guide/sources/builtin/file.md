# File Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

eKuiper provides built-in support for reading file content into the eKuiper processing pipeline. This is useful in scenarios where data is batch-processed or when files need real-time processing by eKuiper. **Note**: The file source supports monitoring either files or directories. If the monitored location is a directory, all files within that directory must be of the same type. When monitoring a directory, it will read files in alphabetical order by the file names.

The File Source Connector allows eKuiper to read data from local files, supporting multiple formats such as JSON, CSV, and line-separated values:

- JSON: Files in standard JSON array format.
- csv: CSV files with comma or custom separators.
- lines: line-separated file.

:::: tabs type:card

::: tab JSON

**Description**: Files in standard JSON array format.

**Example**:

```json
[
  { "id": 1, "name": "John Doe" },
  { "id": 2, "name": "Jane Smith" }
]
```

**Special Note**: If the file consists of line-separated JSON strings, you should define the file type as "lines" and set the format to "json".

:::

::: tab csv

**Description**: Traditional comma-separated values (CSV) files.

**Example**:

```csv
id,name,age
1,John Doe,30
2,Jane Smith,25
```

**Custom Separators**: Some CSV-like files might use different delimiters. For such files, you can specify custom separators. For instance, a space-separated CSV might look like:

```csv
id name age
1 John Doe 30
2 Jane Smith 25
```

:::

::: tab Lines

**Description**: Files with line-separated data. Each line represents a distinct data point.

**Example**:

```text
{"id": 1, "name": "John Doe"}
{"id": 2, "name": "Jane Smith"}
```

**Combining with Formats**: The "lines" file type is versatile and can be used with various data formats. For instance, if you set the format to "protobuf" and provide the appropriate schema, you can parse files containing multiple lines of Protobuf encoded data.

:::

::::

::: tip

When dealing with files that have metadata or non-standard content at the beginning or end, you can leverage the `ignoreStartLines` and `ignoreEndLines` parameters to ensure the main content is parsed correctly.

:::

## Configurations

The connector in eKuiper can be configured with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API](../../../api/restapi/configKey.md), or configuration file. This section focuses on configuring eKuiper connectors with the configuration file.

The configure file for the file source is located at `/etc/sources/file.yaml`.

**Example**

```yaml
default:
  # The type of the file, could be raw, json, csv and lines
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
  # Decompress the file with the specified compression method. Support `gzip`, `zstd` method now.                                                                                                                                                                                                                                           |
  decompression: ""
```

### File Type & Path

- **`fileType`**: Defines the type of file. Supported values are `raw`, `json`, `csv`, and `lines`. Among them, `raw`
  type will read the binary data of the whole file. Usually, the stream format should be binary to for such file type.
- **`path`**: Specifies the directory of the file, either relative to the Kuiper root or an absolute path. Note: Do not include the file name here. The file name should be defined in the stream data source.

### Reading & Sending Intervals

- **`interval`**: Sets the interval, in milliseconds, between file reads. If set to 0, the file is read only once. Since
  v2.1.0, interval 0 means monitor changed the specified file or folder. Once change happens, the changed file will be
  read.
- **`sendInterval`**: Determines the interval, in milliseconds, between sending each event.

### Post-Read Actions

- `actionAfterRead`: Determines the action after reading the file:
  - `0`: Keep the file.
  - `1`: Delete the file.
  - `2`: Move the file to the location specified in `moveTo`.
- **`moveTo`**: Specifies the path to move the file to after reading. Only valid if `actionAfterRead` is set to `2`.

### File Content Configuration (CSV-specific)

- **`hasHeader`**: Indicates if the file has a header line.
- **`columns`**: Defines the column names, particularly useful for CSV files. For instance, `columns: [id, name]`.
- **`ignoreStartLines`**: Specifies the number of lines to be ignored at the beginning of the file. Empty lines will be ignored and not counted.
- **`ignoreEndLines`**: Specifies the number of lines to be ignored at the end of the file. Again, empty lines will be ignored and not counted.

### Decompression

- **`decompression`**: Allows decompression of files. Currently, `gzip` and `zstd` methods are supported.

## Create a Table Source

After setting up your streams, you can integrate them with eKuiper rules to process the data.

::: tip

The File Source connector can operate as either a [stream source](../../streams/overview.md) or a [scan table](../../tables/scan.md) source. When configured as a stream source, you have the option to set the `interval` parameter, allowing for regular updates at specified intervals. While the file source is commonly utilized as a [table](../../../sqls/tables.md) — and it's the default configuration for the "create table" statement — this section will primarily demonstrate its integration as a table source.

:::

For example, to create a table named `table1` with three columns (`name`, `size`, and `id`) and populate this table with data from a JSON file named `lookup.json`, you can work with the code below:

```sql
create table table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="lookup.json", FORMAT="json", TYPE="file");
```

After setting up your streams, you can integrate them with eKuiper rules to process the data.

```sql
CREATE RULE rule1 AS SELECT * FROM fileDemo WHERE temperature > 50 INTO mySink;
```

This rule selects all data from the `fileDemo` stream where the temperature exceeds 50 and sends it to `mySink`.

You can define the file source as the data source either by [REST API](../../../api/restapi/streams.md) or [CLI tool](../../../api/cli/streams.md).

## Tutorial: Parsing File Sources

File sources in eKuiper require parsing of content, which often intersects with format-related stream definitions. To illustrate how eKuiper parses different file formats, let's walk through a couple of examples.

### Read a CSV File with a Custom Separator

While the standard separator for CSV files is a comma, many files utilize custom delimiters in a CSV-like format. Additionally, some of these files designate the first line for column names rather than data values.

```csv
id name age
1 John 56
2 Jane 34
```

Before eKuiper can read the file, you need to specify its type and inform the system about the header. Modify the configuration file (`/etc/sources/file.yaml`) with the following content:

```yaml
csv:
  fileType: csv
  hasHeader: true
```

This configuration indicates that the file is of type `csv` and has a header.

In the stream definition, define a stream with the `DELIMITED` format. The `DELIMITER` parameter allows us to specify the custom separator (in this case, a space).

```SQL
create
stream cscFileDemo () WITH (FORMAT="DELIMITED", DATASOURCE="abc.csv", TYPE="file", DELIMITER=" ", CONF_KEY="csv"
```

This command creates a stream named `csvFileDemo` that reads from the `abc.csv` file, expecting space-separated values.

### Parse Multi-Line JSON Data

Typically, a standard JSON file contains a single JSON object or an array. However, some files consist of multiple JSON objects, each on a separate line.

```text
{"id": 1, "name": "John Doe"}
{"id": 2, "name": "Jane Doe"}
{"id": 3, "name": "John Smith"}
```

To effectively process this file, modify the configuration file (`/etc/sources/file.yaml`) with the following content:

```yaml
jsonlines:
  fileType: lines
```

In the stream definition, set the stream data to be in `JSON` format.

```SQL
create stream linesFileDemo () WITH (FORMAT="JSON", TYPE="file", CONF_KEY="jsonlines")
```

This command configures a stream named `linesFileDemo` to process line-separated JSON data from the source file.

## Tutorial: Monitoring a Folder

In many IoT and data processing scenarios, we need to monitor file changes in a specific folder in real time, such as
the creation of new files or modification of existing files. For example, monitoring screenshot files from a camera and
synchronizing them to the cloud. With eKuiper, we can easily monitor file changes in a folder and process and analyze
these changes in real time. This tutorial will guide you on how to configure eKuiper to monitor a folder and create
corresponding streams and rules to handle file change events. We will monitor the `data/watch` folder and upload newly
created image files to the cloud via MQTT.

### Configuring Monitoring Options

Create a ConfKey named `watch` using the following REST API for use when creating a stream.

```http request
PUT http://{{host}}/metadata/sources/file/confKeys/watch
Content-Type: application/json

{
  "interval": 0,
  "fileType": "raw",
  "path": "data"
}
```

- **interval**: 0 means that we do not use periodic polling but instead use inotify for monitoring.
- **fileType**: The `raw` file type indicates that the file content will be read as binary. Generally, the stream format
  needs to be configured as `binary`.
- **path**: The root directory for file reading. The stream definition can configure specific files or folders relative
  to this root directory.

If you need to automatically delete or move files after reading them, you can add the `actionAfterRead` configuration.

### Creating a File Stream

Use the REST API to create a file stream named `watch`.

```http request
POST http://{{host}}/streams
Content-Type: application/json

{
  "sql": "CREATE STREAM watch() WITH (TYPE=\"file\",FORMAT=\"binary\",DATASOURCE=\"watch\",CONF_KEY=\"watch\", SHARED=\"true\");"
}
```

Where:

- **CONF_KEY** is set to the `watch` created in the previous step.
- **DATASOURCE** is configured to the folder to be monitored. Combined with the `path` configured in CONF_KEY, the full
  path is `data/watch`.
- The format is configured as `binary`.

### Monitoring and Transmission Rule

Use the REST API to create a rule named `ruleWatch`. This rule reads the complete binary data through `self` and sends
it to MQTT.

```http request
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "ruleWatch",
  "name": "Watch image folder and pass through the raw binary data of image to MQTT",
  "sql": "SELECT self FROM watch",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "result",
        "sendSingle": true,
        "format": "binary"
      }
    }
  ]
}
```

### Testing

Subscribe to the MQTT topic `result`, then place image files into the `data/watch` directory. Observe whether the MQTT
receives the binary data of the images. Note: MQTT brokers usually have a size limit for transmission, so you need to
use small image files or increase the size limit of the MQTT broker during testing.
