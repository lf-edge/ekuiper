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
  # 是否并行读取目录中的文件
  parallel: false
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
  # 使用指定的压缩方法解压缩文件。现在支持`gzip`、`zstd` 方法。                                                                                                                                                                                                                                         |
  decompression: ""
```

### 文件源

文件源支持监控文件或文件夹。如果被监控的位置是一个文件夹，那么该文件夹中的所有文件必须是同一类型。当监测一个文件夹时，它将按照文件名的字母顺序来读取文件。

支持的文件类型有：

- json：标准的JSON数组格式文件。见[例子](https://github.com/lf-edge/ekuiper/tree/master/internal/topo/source/test/test.json)。如果文件格式是一个以行分隔的JSON字符串，它需要以 `lines` 格式定义。
- csv：支持逗号分隔的 csv 文件，也支持自定义分隔符。
- lines：以行分隔的文件。每行的解码方法可以通过流定义中的 `format` 参数来定义。例如，对于一个按行分隔的 JSON 字符串文件，文件类型应设置为 `lines`，格式应设置为 `json`，表示单行的格式为 json。

有些文件可能有大部分数据是标准格式，但在文件的开头和结尾行有一些元数据。用户可以使用 `ignoreStartLines` 和 `ignoreEndLines` 参数来删除非标准的开头和结尾的非标准部分，这样上述文件类型就可以被解析了。

### 示例

文件源涉及对文件内容的解析，同时解析格式与数据流中的格式定义相关。我们用一些例子来描述如何结合文件类型和格式设置来解析文件源。

#### 读取自定义分隔符的 CSV 文件

标准的 csv 文件，分隔符是一个逗号，但是有大量的文件使用类 csv 格式，但使用自定义的分隔符。另外，一些类 csv 的文件在第一行定义了列名，而不是数据，如下例所示。

```csv
id name age
1 John 56
2 Jane 34
```

该文件第一行为文件头，定义了文件的列名。读取这样的文件时，配置文件如下，需要指定文件有一个头。

```yaml
csv:
  fileType: csv
  hasHeader: true
```

在流定义中，将流数据设置为 `DELIMITED` 格式，用 `DELIMITER` 参数指定分隔符为空格。

```SQL
create
stream cscFileDemo () WITH (FORMAT="DELIMITED", DATASOURCE="abc.csv", TYPE="file", DELIMITER=" ", CONF_KEY="csv"
```

#### 读取多行 JSON 数据

对于一个标准的 JSON 文件，整个文件应该是一个 JSON 对象或一个数组。在实践中，我们经常需要解析包含多个 JSON 对象的文件。这些文件实际上本身不是合法的 JSON 格式，但每行都是合法的 JSON 格式，可认为是多行JSON数据。

```text
{"id": 1, "name": "John Doe"}
{"id": 2, "name": "Jane Doe"}
{"id": 3, "name": "John Smith"}
```

读取这种格式的文件时，配置中的文件类型设置为 `lines`。

```yaml
jsonlines:
  fileType: lines
```

在流定义中，设置流数据为`JSON`格式。

```SQL
create stream linesFileDemo () WITH (FORMAT="JSON", TYPE="file", CONF_KEY="jsonlines"
```

此外，lines 文件类型可以与任何格式相结合。例如，如果你将格式设置为 protobuf，并且配置模式，它可以用来解析包含多个 Protobuf 编码行的数据。
