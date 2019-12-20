# 规则

规则由JSON定义，下面是一个示例。

```json
{
  "id": "rule1",
  "sql": "SELECT demo.temperature, demo1.temp FROM demo left join demo1 on demo.timestamp = demo1.timestamp where demo.temperature > demo1.temp GROUP BY demo.temperature, HOPPINGWINDOW(ss, 20, 10)",
  "actions": [
    {
      "log": {}
    },
    {
      "mqtt": {
        "server": "tcp://47.52.67.87:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```

创建规则需要以下3个参数。

## 参数

| 参数名 | 是否可选 | 说明                |
| ------------- | -------- | ------------------------------------------------------------ |
| id | false   | 规则id |
| sql        | false   | 为规则运行的sql查询 |
| actions           | false    | Sink动作数组 |
| options           | 是       | A map of options        |

## id

规则的标识。 规则名称不能在同一Kuiper实例中重复。

## sql

为规则运行的sql查询。

- Kuiper支持嵌入式MQTT源，有关更多详细信息，请参阅[MQTT source stream](sources/mqtt.md)。
- 有关Kuiper SQL的更多信息，请参阅[SQL](../sqls/overview.md)。
- 可以自定义来源，请参阅 [extension](../extension/overview.md)了解更多详细信息。

### 动作

当前，支持两种操作： [log](sinks/logs.md) 、[mqtt](sinks/mqtt.md) 和 [rest](sinks/rest.md)。 每个动作可以定义自己的属性。当前有两个公共属性:

| 属性名 | 类型和默认值 | 描述                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| concurrency | int: 1   | 设置运行的线程数。 |
| runAsync        | bool:false   | 设置是否异步运行输出操作以提升性能。请注意，异步运行的情况下，输出结果顺序不能保证。  |

可以自定义动作以支持不同种类的输出，有关更多详细信息，请参见 [extension](../extension/overview.md) 。

### 选项
当前的选项包括：

| 选项名 | 类型和默认值 | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| isEventTime | 布尔值：false | 使用事件时间还是将时间用作事件的时间戳。 如果使用事件时间，则将从有效负载中提取时间戳。 必须通过[stream]([extension](../sqls/streams.md))定义指定时间戳记。 |
| lateTolerance        | int64:0   | 在使用事件时间窗口时，可能会出现元素延迟到达的情况。 LateTolerance可以指定在删除元素之前可以延迟多少时间（单位为毫秒）。 默认情况下，该值为0，表示后期元素将被删除。 |