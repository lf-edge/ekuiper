# 文件数据源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper 内置支持文件数据源，可将文件内容读入 eKuiper 处理管道，适用于需要对数据进行批量处理或需要对文件进行实时处理的场景。

eKuiper 支持  JSON、CSV 或以行分隔的文件：

- JSON：标准 JSON 数组格式文件。
- CSV：支持逗号或其他自定义分隔符的 CSV 文件。
- lines：以行分隔的文件。

**注意**：文件源支持监控文件或文件夹。如果被监控的位置是一个文件夹，那么该文件夹中的所有文件必须是同一类型。当监测一个文件夹时，它将按照文件名的字母顺序来读取文件。

:::: tabs type:card

::: tab JSON

**描述**：标准 JSON 数组格式文件。

**示例**

```json
[
  {"id": 1, "name": "John Doe"},
  {"id": 2, "name": "Jane Smith"}
]
```

**注意**：如果文件格式是一个以行分隔的 JSON 字符串，则需要定义为 `lines` 格式。

:::

::: tab csv

**描述**：包含逗号分隔符的（CSV）文件。

**示例**

```csv
id,name,age
1,John Doe,30
2,Jane Smith,25
```

**自定义分隔符**：eKuiper 也支持自定义分隔符，如空格：

```csv
id name age
1 John Doe 30
2 Jane Smith 25
```

:::

::: tab lines

**描述**：以行分隔的文件。

**示例**

```text
{"id": 1, "name": "John Doe"}
{"id": 2, "name": "Jane Smith"}
```

**提示**：lines 文件类型可以与任何格式相结合。例如，如果你将格式设置为 protobuf，并且配置模式，它可以用来解析包含多个 Protobuf 编码行的数据。

:::

::::

::: tip

在处理具有元数据或非标准内容的文件时，您可以利用 `ignoreStartLines` 和 `ignoreEndLines` 参数来删除非标准的开头和结尾的非标准部分，以便正确解析相关内容。

:::

## 配置

eKuiper 连接器可以通过[环境变量](../../../configuration/configuration.md#environment-variable-syntax)、[REST API](../../../api/restapi/configKey.md) 或配置文件进行配置，本节将介绍配置文件的使用方法。

文件数据源连接器的配置文件位于 `/etc/sources/file.yaml`。

**示例**

```yaml
default:
  # 文件的类型，支持 json， csv 和 lines
  fileType: json
  # 文件以 eKuiper 为根目录的目录或文件的绝对路径。
  # 请勿在此处包含文件名。文件名应在流数据源中定义
  path: data
  # 读取文件的时间间隔，单位为ms。如果只读取一次，则将其设置为 0
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
  # 使用指定的压缩方法解压缩文件。现在支持`gzip`、`zstd` 方法。
  decompression: ""
```

### 文件类型和路径

- **`fileType`**：定义文件的类型，可选值为 `json`、`csv` 和 `lines`。
- **`path`**：指定文件的目录，相对于 eKuiper 根目录的相对路径或绝对路径。注意：这里不要包含文件名，文件名应在流数据源中定义。

### 读取和发送间隔

- **`interval`**：设置文件读取之间的间隔，单位为毫秒。如果设置为0，文件只读取一次。
- **`sendInterval`**：读取后，两条数据发送的间隔时间，单位为毫秒。

### 并行处理

- **`parallel`**：确定目录中的文件是否应并行读取。如果设置为 `true`，目录中的文件将并行读取。

### 读后操作

- `actionAfterRead`：确定读取文件后的操作：
  - `0`：保留文件。
  - `1`：删除文件。
  - `2`：将文件移至`moveTo`指定的位置。
- **`moveTo`**：指定读取后将文件移至的路径。仅在`actionAfterRead`设置为 `2` 时有效。

### 文件内容配置 (CSV 格式)

- **`hasHeader`**：指定文件是否有表头行。
- **`columns`**：定义列名，特别适用于CSV文件。例如，`columns: [id, name]`。
- **`ignoreStartLines`**：指定文件开始处要忽略的行数。空行将被忽略且不计算在内。
- **`ignoreEndLines`**：指定文件末尾要忽略的行数。同样，空行将被忽略且不计算在内。

### 解压缩

- **`decompression`**：允许解压缩文件。目前支持 `gzip` 及 `zstd`。

## 创建表式数据源

完成连接器的配置后，后续可通过创建流将其与 eKuiper 规则集成。文件数据源连接器可以作为 [流式](../../streams/overview.md)或[扫描表类数据源](../../tables/scan.md)使用。当作为流式数据源时，此时通常需要设置 `interval` 参数以定时拉取更新。但文件源更常用作[表格](../../../sqls/tables.md)， 并且采用 create table 语句的默认类型。

您可通过 [REST API](../../../api/restapi/streams.md) 或 [CLI](../../../api/cli/streams.md) 工具在 eKuiper 中创建文件数据源。本节将以表类数据源为例进行说明。

例如，要创建一个名为 `table1` 的表，其中包含三列（`name`、`size`和 `id`），并使用 `lookup.json` 文件作为数据源：

```sql
create table table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="lookup.json", FORMAT="json", TYPE="file");
```

创建完成后，您可将其与 eKuiper 规则集成以处理数据。

```sql
CREATE RULE rule1 AS SELECT * FROM fileDemo WHERE temperature > 50 INTO mySink;
```

根据设定规则，我们将选择 `fileDemo` 数据流中所有温度超过 50 的数据，并将其发送到  `mySink`。

## 教程：解析文件源

文件源涉及对文件内容的解析，同时解析格式与数据流中的格式定义相关。本节将通过一些示例来描述如何结合文件类型和格式设置来解析文件源。

### 读取自定义分隔符的 CSV 文件

标准的 csv 文件会采用逗号作为分隔符，但也存在使用自定义分隔符的情况。此外，一些类 csv 的文件会在第一行定义列名，而非数据，如下例所示：

```csv
id name age
1 John 56
2 Jane 34
```

因此，我们需要进行如下配置，指定其类型并告知表头信息，在本例中，我们可按如下示例修改 `/etc/sources/file.yaml` 文件：

```yaml
csv:
  fileType: csv
  hasHeader: true
```

以上配置表明文件是 `csv` 类型，并且有表头。

在定义流时，需要采用 `DELIMITED`格式。`DELIMITER`参数支持我们指定自定义分隔符，即本例中的空格。

```SQL
create
stream cscFileDemo () WITH (FORMAT="DELIMITED", DATASOURCE="abc.csv", TYPE="file", DELIMITER=" ", CONF_KEY="csv"
```

通过以上命令，我们创建了一个名为 `csvFileDemo` 的流，该流将从 `abc.csv` 文件中读取数据，预期分隔符为空格。

### 读取多行 JSON 数据

对于一个标准的 JSON 文件，整个文件应该是一个 JSON 对象或一个数组。在实践中，我们经常需要解析包含多个 JSON 对象的文件。这些文件实际上本身不是合法的 JSON 格式，但每行都是合法的 JSON 格式，可认为是多行JSON数据。

```text
{"id": 1, "name": "John Doe"}
{"id": 2, "name": "Jane Doe"}
{"id": 3, "name": "John Smith"}
```

读取这种格式的文件时，应在 `/etc/sources/file.yaml`  文件中进行如下配置：

```yaml
jsonlines:
  fileType: lines
```

在定义流时，设置流数据为 `JSON`格式。

```SQL
create stream linesFileDemo () WITH (FORMAT="JSON", TYPE="file", CONF_KEY="jsonlines")
```

此命令将创建一个名为 `linesFileDemo` 的流，并从源文件拉取以行分隔的 JSON 数据。
