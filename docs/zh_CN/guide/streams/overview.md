# 流

流是 eKuiper 中数据源连接器的运行形式。它必须指定一个源类型来定义如何连接到外部资源。

当作为一个流使用时，源必须是无界的。流的作用就像规则的触发器。每个事件都会触发规则中的计算。

与关系型数据库不同，eKuiper 不需要一个预先建立的模式。这使得它可以适应无模式的数据，这在物联网和边缘场景中很常见。在处理固定类型的数据流时，用户也可以像数据库一样定义模式，以便在编译时获得更多验证和 SQL 优化。在大多数情况下，无模式跳过了数据加载过程中的数据验证，这可能获得更好的性能。

## 流定义

```sql
CREATE STREAM   
    stream_name   
    ( column_name <data_type> [ ,...n ] )
    WITH ( property_name = expression [, ...] );
```

流定义是一个 SQL 语句。它由两部分组成。

1. 流的模式定义。其语法与 SQL 表定义相同。这里的模式是可选的。如果它是空的，则流是无模式的。
2. 在 WITH 子句中定义连接器类型和行为的属性，如序列化格式。

### 流定义中的模式

模式定义是可选的。只有当输入的数据是固定类型并且需要强大的验证时，才需要它。

在 eKuiper 中，每个列或表达式都有一个相关的数据类型。 数据类型描述（约束）该类型的列可以容纳的一组值或该类型可以产生的表达式。

同时，当数据源的格式为 json 时，定义流的模式信息将有助于在解析 json 数据时仅将模式定义中的数据被解析出来。当数据源中单条信息的结构较为复杂或者较大且模式定义中所需要的信息明确并简单时，解析仅需的 json 数据将极大的降低单条数据的处理时间，从而提升性能。

以下是支持的数据类型的列表。

| #   | 数据类型     | 说明                                                             |
|-----|----------|----------------------------------------------------------------|
| 1   | bigint   | 整数型。                                                        |
| 2   | float    | 浮点型。                                                        |
| 3   | string   | 文本值，由 Unicode 字符组成。                                     |
| 4   | datetime | 日期时间类型。                                                   |
| 5   | boolean  | 布尔类型，值可以是`true` 或者 `false`。                            |
| 6   | bytea    | 用于存储二进制数据的字节数组。如果在格式为 "JSON" 的流中使用此类型，则传入的数据需要为 base64 编码的字符串。 |
| 7   | array    | 数组类型可以是任何简单类型，数组类型或结构类型。                                       |
| 8   | struct   | 复杂类型。                                                      |

### 流属性

| 属性名称             | 可选  | 说明                                                                                                                                                                      |
|------------------|-----|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| DATASOURCE       | 否   | 取决于不同的源类型；如果是 MQTT 源，则为 MQTT 数据源主题名；其它源请参考相关的文档。                                                                                                                        |
| FORMAT           | 是   | 传入的数据类型，支持 "JSON", "PROTOBUF" 和 "BINARY"，默认为 "JSON" 。关于 "BINARY" 类型的更多信息，请参阅 [Binary Stream](#二进制流)。该属性是否生效取决于源的类型，某些源自身解析的时固定私有格式的数据，则该配置不起作用。可支持该属性的源包括 MQTT 和 ZMQ 等。 |
| SCHEMAID         | 是   | 解码时使用的模式，目前仅在格式为 PROTOBUF 的情况下使用。                                                                                                                                       |
| DELIMITER        | 是   | 仅在使用 `delimited` 格式时生效，用于指定分隔符，默认为逗号。                                                                                                                                   |
| KEY              | 是   | 保留配置，当前未使用该字段。 它将用于 GROUP BY 语句。                                                                                                                                        |
| TYPE             | 是   | 源类型，如未指定，值为 "mqtt"。                                                                                                                                                     |
| StrictValidation | 是   | 针对流模式控制消息字段的验证行为。 有关更多信息，请参见 [Strict Validation](#strict-validation)                                                                                                    |
| CONF_KEY         | 是   | 如果需要配置其他配置项，请在此处指定 config 键。 有关更多信息，请参见 [MQTT stream](../sources/builtin/mqtt.md) 。                                                                                     |
| SHARED           | 是   | 是否在使用该流的规则中共享源的实例                                                                                                                                                       |
| TIMESTAMP        | 是   | 代表该事件时间戳的字段名。如果有设置，则使用此流的规则将采用事件时间；否则将采用处理时间。详情请看[时间戳管理](../../sqls/windows.md#时间戳管理)。                                                                                  |
| TIMESTAMP_FORMAT | 是   | 字符串和时间格式转换时使用的默认格式。                                                                                                                                                     |

**示例1**

```sql
my_stream 
  (id bigint, name string, score float)
WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");
```

该流将订阅 MQTT 主题 `topic/temperature`，服务器连接使用配置文件 `$ekuiper/etc/mqtt_source.yaml` 中默认部分的 server 键。

- 有关更多信息，请参见 [MQTT source](../sources/builtin/mqtt.md)

**示例2**

```sql
demo (
        USERID BIGINT,
        FIRST_NAME STRING,
        LAST_NAME STRING,
        NICKNAMES ARRAY(STRING),
        Gender BOOLEAN,
        ADDRESS STRUCT(STREET_NAME STRING, NUMBER BIGINT),
    ) WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo");
```

流将订阅 MQTT 主题 `test/`，服务器连接使用配置文件 `$ekuiper/etc/mqtt_source.yaml` 中 demo 部分的设置。

**示例3**

```sql
demo () WITH (DATASOURCE="test/", FORMAT="protobuf", SCHEMAID="proto1.Book");
```

流将订阅 MQTT 主题 `test/`，使用 PROTOBUF 格式，根据在 `$ekuiper/data/schemas/protobuf/schema1.proto` 文件中的 `Book` 定义对流入的数据进行解码。其中，模式的管理详见[模式注册表](../serialization/serialization.md#模式)。

- 有关更多信息，请参见 [MQTT source](../sources/builtin/mqtt.md)

- 有关规则和流管理的更多信息，请参见 [规则和流 CLI docs](../../api/cli/overview.md)

### 共享源实例

默认情况下，每个规则会创建自己的源实例。在某些场景中，用户需要不同的规则处理完全相同的数据流。例如，在处理传感器的温度数据时，用户可能需要一个规则，当一段时间的平均温度大于30度时触发警告；而另一个规则则是当一段时间的平均温度小于0度时触发警告。使用默认配置时，两个规则各自独立实例化源实例。由于网络延迟等原因，规则可能得到不同顺序，甚至各有缺失数据的数据流，从而在不同的数据维度中计算平均值。通过配置共享源实例，用户可以确保两个规则处理完全相同的数据。同时，由于节省了额外的源实例开销，规则的性能也能得到提升。

使用共享源实例模式，只需要共享源实例的流时，将其 `SHARED` 属性设置为 true 。

```text
demo (
        ...
    ) WITH (DATASOURCE="test", FORMAT="JSON", KEY="USERID", SHARED="true");
```

## 数据结构

流的数据结构（schema）包含两个部分。一个是在数据源定义中定义的数据结构，即逻辑数据结构；另一个是在使用强类型数据格式时指定的 SchemaId 即物理数据结构，例如 Protobuf 和 Custom 格式定义的数据结构。

整体上，我们将支持3种递进的数据结构方式：

1. Schemaless，用户无需定义任何形式的 schema，主要用于弱结构化数据流，或数据结构经常变化的情况。
2. 仅逻辑结构，用户在 source 层定义 schema，多用于弱类型的编码方式，例如最常用的 JSON。适用于用户的数据有固定或大致固定的格式，同时不想使用强类型的数据编解码格式。使用这种方式的情况下，可以可通过 StrictValidation 参数配置是否进行数据验证和转换。
3. 物理结构，用户使用 protobuf 或者 custom 格式，并定义 schemaId。此时，数据结构的验证将由格式来实现。

逻辑结构和物理结构定义都用于规则创建的解析和载入阶段的 SQL 语法验证以及运行时优化等。推断后的数据流的数据结构可通过 [Schema API](../../api/restapi/streams.md#获取数据结构) 获取。

### Strict Validation

仅用于逻辑结构的数据流。若设置 strict validation，则规则运行中将根据逻辑结构对字段存在与否以及字段类型进行校验。若数据格式完好，建议关闭验证。

### Schema-less 流

如果流的数据类型未知或不同，我们可以不使用字段来定义它。 这称为 schema-less。 通过将字段设置为空来定义它。

```sql
schemaless_stream 
  ()
WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");
```

Schema-less 流字段数据类型将在运行时确定。 如果在不兼容子句中使用该字段，则会抛出运行时错误并将其发送到目标。 例如，`where temperature > 30`。 一旦温度不是数字，将错误发送到目标。

有关 SQL 语言的更多信息，请参见 [查询语言元素](../../sqls/query_language_elements.md) 。

### 二进制流

对于二进制数据流，例如图像或者视频流，需要指定数据格式为 "BINARY" 。二进制流的数据为一个二进制数据块，不区分字段。所以，其流定义必须仅有一个 `bytea` 类型字段。如下流定义示例中，二进制流的数据将会解析为 `demoBin` 流中的 `image` 字段。

```sql
demoBin (
    image BYTEA
) WITH (DATASOURCE="test/", FORMAT="BINARY");
```

如果 "BINARY" 格式流定义为 schemaless，数据将会解析到默认的名为 `self` 的字段。
