# 内存源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>
<span style="background:green;color:white">lookup table source</span>

内存源通过主题消费由 [内存目标](../../sinks/builtin/memory.md) 生成的事件。该主题类似于 pubsub 主题，例如 mqtt，因此可能有多个内存目标发布到同一主题，也可能有多个内存源订阅同一主题。 内存动作的典型用途是形成[规则管道](../../rule_pipeline.md)。内存动作和内存源之间的数据传输采用内部格式，不经过编解码以提高效率。因此，内存源的`format`属性会被忽略。

主题没有配置属性，由流数据源属性指定，如以下示例所示：

```text
CREATE STREAM table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="devices/result", TYPE="memory");
```

## 主题通配符

内存源也支持主题通配符，与 mqtt 主题类似。 目前，支持两种通配符。

**+** : 单级通配符替换一个主题等级。
**#**: 多级通配符涵盖多个主题级别，只能在结尾使用。

示例：
1. `home/device1/+/sensor1`
2. `home/device1/#`

## 查询表

内存源支持查询表。下面是一个针对内存主题 "topicName" 创建查询表的例子。注意，作为查询表使用时，`KEY` 属性是必须的，它将作为虚拟表的主键来加速查询。

```text
CREATE TABLE alertTable() WITH (DATASOURCE="topicName", TYPE="memory", KIND="lookup", KEY="id")
```

在创建一个内存查询表后，它将开始积累由键字段索引的内存主题的数据。它将一直独立于规则运行。每个主题和键对将有一个虚拟表的内存拷贝。所有引用同一表或具有相同主题/键对的内存表的规则将共享同一数据副本。

内存查询表可以像多个规则之间的管道一样使用，这与[规则管道](../../rule_pipeline.md)的概念类似。它可以在内存中存储任何流类型的历史，以便其他流可以与之合作。通过与[可更新的内存动作](../../sinks/builtin/memory.md#更新)一起工作，表格内容可以被更新。