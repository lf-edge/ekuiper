# 内存源

内存源通过主题消费由 [内存目标](../../sinks/builtin/memory.md) 生成的事件。该主题类似于 pubsub 主题，例如 mqtt，因此可能有多个内存目标发布到同一主题，也可能有多个内存源订阅同一主题。 内存动作的典型用途是形成[规则管道](../../rule_pipeline.md)。

主题没有配置属性，由流数据源属性指定，如以下示例所示：

```text
CREATE TABLE table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="devices/result", FORMAT="json", TYPE="memory");
```

## 主题通配符

内存源也支持主题通配符，与 mqtt 主题类似。 目前，支持两种通配符。

**+** : 单级通配符替换一个主题等级。
**#**: 多级通配符涵盖多个主题级别，只能在结尾使用。

示例：
1. `home/device1/+/sensor1`
2. `home/device1/#`