# 内存数据源

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>
<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>

内存源通过主题消费由 [内存 Sink](../../sinks/builtin/memory.md) 生成的事件，适用于需要快速数据检索而无需磁盘或外部服务读取开销的场景，它的典型使用场景是形成[规则管道](../../rules/rule_pipeline.md)。

使用内存数据源时，无需额外配置，支持作为流式数据源、扫描表数据源或查找表数据源来使用。

## 创建流式数据源

当作为[流数据源](../../streams/overview.md)时，内存连接器会持续从指定的内存主题中提取数据，因此非常适合实时数据处理。

**示例**：

```sql
CREATE STREAM stream1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="devices/result", FORMAT="json", TYPE="memory");
```

在此示例中，我们定义了一个内存流数据源，用于连续从 `devices/result` 内存主题中提取数据。

## 创建扫描表数据源

在需要分析静态数据集时，内存源连接器可以作为[扫描表数据源](../../tables/scan.md)来使用。

**示例**：

```sql
CREATE TABLE memoryTableDemo () WITH (DATASOURCE="topicB", FORMAT="JSON", TYPE="memory");
```

在这个示例中，`memoryTableDemo` 数据源表可查询 `topicB` 内存主题下的 JSON 数据。

## 创建查找表数据源

内存源支持用作查询表，此时，主要具备如下优势：

- **独立性**：内存查找表独立于任何规则操作。即使规则被修改或删除，内存查找表中的数据也不受影响。
- **数据共享**：如果多个规则引用相同的表，或者存在具有相同主题/键对的多个内存表，则它们全部共享相同的数据集，确保了不同规则之间的一致性，简化了数据访问。
- **与内存 Sink 集成**：内存查找表可通过与[可更新的内存 Sink](../../sinks/builtin/memory.md#updatable-sink) 集成，保证内容的实时性。
- **规则管道**：内存查找表可以作为多个规则之间的桥梁，类似于规则管道的概念。它使一个流能够将历史数据存储在内存中，其他流可以访问和利用这些数据，因此适用于需要结合历史数据和实时数据进行决策的场景。

**示例：**

```sql
CREATE TABLE memoryLookupTableDemo () WITH (DATASOURCE="topicC", FORMAT="JSON", TYPE="memory");
```

注意，作为查询表使用时，还应配置 `KEY` 属性，它将作为虚拟表的主键来加速查询。创建完成后，内存查找表将开始从指定的内存主题累积数据，并通过 `KEY` 字段进行索引，允许快速检索。

## 内存数据源中的主题

内存数据源中的“主题”表示不同的内存数据通道。当定义流或表时，用户可以使用 `DATASOURCE` 属性来锁定希望访问的内存主题。

### 主题通配符

与 MQTT 主题类似，内存源也支持主题通配符：

- **+** : 单级通配符替换一个主题等级。
- **#**: 多级通配符涵盖多个主题级别，只能在结尾使用。

示例：

1. `home/device1/+/sensor1`
2. `home/device1/#`

## 通过内存源构建规则管道

内存源的典型用途在于构建[规则管道](../../rules/rule_pipeline.md)。这样的管道允许将多个规则链接起来，使得一个规则的输出成为另一个规则的输入。此外，内存动作和内存源之间的数据传输采用内部格式，不经过编解码以提高效率。因此，内存源的 `format` 属性会被忽略。
