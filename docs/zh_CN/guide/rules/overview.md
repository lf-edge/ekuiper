# 规则

规则由 JSON 定义，下面是一个示例。

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

创建规则需要以下参数。

## 参数

| 参数名     | 是否可选                  | 说明                                |
|---------|-----------------------|-----------------------------------|
| id      | 否                     | 规则 id, 规则 id 在同一 eKuiper 实例中必须唯一。 |
| name    | 是                     | 规则显示的名字或者描述。                      |
| sql     | 如果 graph 未定义，则该属性必须定义 | 为规则运行的 sql 查询                     |
| actions | 如果 graph 未定义，则该属性必须定义 | Sink 动作数组                         |
| graph   | 如果 sql 未定义，则该属性必须定义   | 规则有向无环图的 JSON 表示                  |
| options | 是                     | 选项列表                              |

## 规则逻辑

一个规则代表了一个流处理流程，定义了从将数据输入流的数据源到各种处理逻辑，再到将数据输入到外部系统的动作。

有两种方法来定义规则的业务逻辑。要么使用SQL/动作组合，要么使用新增加的图API。

### SQL 规则

通过指定 `sql` 和 `actions` 属性，我们可以以声明的方式定义规则的业务逻辑。其中，`sql` 定义了针对预定义流运行的 SQL 查询，这将转换数据。然后，输出的数据可以通过 `action` 路由到多个位置。

#### SQL

最简单的规则 SQL 如 `SELECT * FROM demo`。它有类似于 ANSI SQL 的语法，并且可以利用 eKuiper 运行时提供的丰富的运算符和函数。参见[SQL](../../sqls/overview.md)获取更多 eKuiper SQL 的信息。

大部分的 SQL 子句都是定义逻辑的，除了负责指定流的 `FROM` 子句。在这个例子中，`demo` 是一个流。通过使用连接子句，可以有多个流或流/表。作为一个流引擎，一个规则中必须至少有一个流。

因此，这里的 SQL 查询语句实际上定义了两个部分。

- 要处理的流或表。
- 如何处理。

在使用SQL规则之前，必须事先定义流。请查看 [streams](../streams/overview.md) 了解详情。

#### 动作

动作部分定义了一个规则的输出行为。每个规则可以有多个动作。一个动作是一个 sink 连接器的实例。当定义动作时，键是 sink 连接器的类型名称，而值是其属性。

eKuiper 已经内置了丰富的 sink connector 类型，如 mqtt、rest 和 file 。用户也可以扩展更多的 sink 类型来用于规则动作中。每种sink类型都有自己的属性集。更多细节，请查看 [sink](../sinks/overview.md)。

### 图规则

从 eKuiper 1.6.0 开始, eKuiper 在规则模型中提供了图规则 API 作为创建规则的另一种方式。该属性以 JSON 格式定义了一个规则的 DAG。它很容易直接映射到 GUI 编辑器中的图形，并适合作为拖放用户界面的后端。下面是一个图形规则定义的例子。

```json
{
  "id": "rule1",
  "name": "Test Condition",
  "graph": {
    "nodes": {
      "demo": {
        "type": "source",
        "nodeType": "mqtt",
        "props": {
          "datasource": "devices/+/messages"
        }
      },
      "humidityFilter": {
        "type": "operator",
        "nodeType": "filter",
        "props": {
          "expr": "humidity > 30"
        }
      },
      "logfunc": {
        "type": "operator",
        "nodeType": "function",
        "props": {
          "expr": "log(temperature) as log_temperature"
        }
      },
      "tempFilter": {
        "type": "operator",
        "nodeType": "filter",
        "props": {
          "expr": "log_temperature < 1.6"
        }
      },
      "pick": {
        "type": "operator",
        "nodeType": "pick",
        "props": {
          "fields": ["log_temperature as temp", "humidity"]
        }
      },
      "mqttout": {
        "type": "sink",
        "nodeType": "mqtt",
        "props": {
          "server": "tcp://${mqtt_srv}:1883",
          "topic": "devices/result"
        }
      }
    },
    "topo": {
      "sources": ["demo"],
      "edges": {
        "demo": ["humidityFilter"],
        "humidityFilter": ["logfunc"],
        "logfunc": ["tempFilter"],
        "tempFilter": ["pick"],
        "pick": ["mqttout"]
      }
    }
  }
}
```

`graph` 属性是一个 json 结构，其中 `nodes` 定义图形中呈现的节点，`topo` 定义节点之间的边缘。节点类型可以是内置的节点类型，如窗口节点和过滤器节点等。它也可以是来自插件的用户定义的节点。请参考 [graph rule](./graph_rule.md) 了解更多细节。

## 选项

当前的选项包括：

| 选项名                | 类型和默认值     | 说明                                                                                             |
|--------------------|------------|------------------------------------------------------------------------------------------------|
| isEventTime        | bool:false | 使用事件时间还是将时间用作事件的时间戳。 如果使用事件时间，则将从有效负载中提取时间戳。 必须通过 [stream](../../sqls/streams.md) 定义指定时间戳记。    |
| lateTolerance      | int64:0    | 在使用事件时间窗口时，可能会出现元素延迟到达的情况。 LateTolerance 可以指定在删除元素之前可以延迟多少时间（单位为 ms）。 默认情况下，该值为0，表示后期元素将被删除。   |
| concurrency        | int: 1     | 一条规则运行时会根据 sql 语句分解成多个 plan 运行。该参数设置每个 plan 运行的线程数。该参数值大于1时，消息处理顺序可能无法保证。                      |
| bufferLength       | int: 1024  | 指定每个 plan 可缓存消息数。若缓存消息数超过此限制，plan 将阻塞消息接收，直到缓存消息被消费使得缓存消息数目小于限制为止。此选项值越大，则消息吞吐能力越强，但是内存占用也会越多。 |
| sendMetaToSink     | bool:false | 指定是否将事件的元数据发送到目标。 如果为 true，则目标可以获取元数据信息。                                                       |
| sendError          | bool: true | 指定是否将运行时错误发送到目标。如果为 true，则错误会在整个流中传递直到目标。否则，错误会被忽略，仅打印到日志中。                                    |
| qos                | int:0      | 指定流的 qos。 值为0对应最多一次； 1对应至少一次，2对应恰好一次。 如果 qos 大于0，将激活检查点机制以定期保存状态，以便可以从错误中恢复规则。                 |
| checkpointInterval | int:300000 | 指定触发检查点的时间间隔（单位为 ms）。 仅当 qos 大于0时才有效。                                                          |
| restartStrategy    | 结构         | 指定规则运行失败后自动重新启动规则的策略。这可以帮助从可恢复的故障中回复，而无需手动操作。请查看[规则重启策略](#规则重启策略)了解详细的配置项目。                    |
| cron               | string: ""   | 指定规则的周期性触发策略，该周期通过 [cron 表达式](https://zh.wikipedia.org/wiki/Cron) 进行描述。 |
| duration           | string: ""   | 指定规则的运行持续时间，只有当指定了 cron 后才有效。duration 不应该超过两次 cron 周期之间的时间间隔，否则会引起非预期的行为。   |

有关 `qos` 和 `checkpointInterval` 的详细信息，请查看[状态和容错](./state_and_fault_tolerance.md)。

可以在 `rules` 下属的 `etc/kuiper.yaml` 中全局定义规则选项。 规则 json 中定义的选项将覆盖全局设置。

### 规则重启策略

规则重启策略的配置项包括：

| 选项名          | 类型和默认值     | 说明                                                        |
|--------------|------------|-----------------------------------------------------------|
| attempts     | int: 0     | 最大重试次数。如果设置为0，该规则将立即失败，不会进行重试。                            |
| delay        | int: 1000  | 默认的重试间隔时间，以毫秒为单位。如果没有设置 `multiplier`，重试的时间间隔将固定为这个值。       |
| maxDelay     | int: 30000 | 重试的最大间隔时间，单位是毫秒。只有当 `multiplier` 有设置时，从而使得每次重试的延迟都会增加时才会生效。 |
| multiplier   | float: 2   | 重试间隔时间的乘数。                                                |
| jitterFactor | float: 0.1 | 添加或减去延迟的随机值系数，防止在同一时间重新启动多个规则。                            |

这些选项的默认值定义于 `etc/kuiper.yaml` 配置文件，可通过修改该文件更改默认值。

### 周期性规则

规则支持周期性的启动、运行和暂停。在 options 中，`cron` 表达了周期性规则的启动策略，如每 1 小时启动一次，而 `duration` 则表达了每次启动规则时的运行时间，如运行 30 分钟。

当 `cron` 是每 1 小时一次，而 `duration` 是 30 分钟时，那么该规则会每隔 1 小时启动一次，每次运行 30 分钟后便被暂停，等待下一次的启动运行。

通过 [停止规则](../../api/restapi/rules.md#停止规则) 停止一个周期性规则时，便会将该规则从周期性调度器中移除，从而不再被调度运行。如果该周期性规则正在运行，那么该运行也会被暂停。

## 查看规则状态

当一条规则被部署到 eKuiper 中后，我们可以通过规则指标来了解到当前的规则运行状态。

### 查看规则状态

我们可以通过 rest api 来得到所有规则的运行状态，与单条规则详细状况。

获得所有规则的状态可以通过 [展示规则](../../api/restapi/rules.md#展示规则) 了解，而获取单条规则状态可以通过 [获取规则的状态](../../api/restapi/rules.md#获取规则的状态)。

### 了解规则运行的状态指标

对于以下规则:

```json
{
  "id": "rule",
  "sql": "select * from demo",
  "actions": [
     {
      "mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "devices/+/messages",
        "qos": 1,
        "clientId": "demo_001",
        "retained": false
      }
    }
  ]
}
```

我们可以通过上述 `获取规则的状态` 获取到以下内容:

```json
{
  "status": "running",
  "source_demo_0_records_in_total": 0,
  "source_demo_0_records_out_total": 0,
  ......
  "op_2_project_0_records_in_total": 0,
  "op_2_project_0_records_out_total": 0,
  ......
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  ......
}
```

其中 `status` 代表了 rule 当前的运行状态，`running` 代表规则正在运行。

而之后的监控项则代表了规则运行过程中，各个算子的运行情况，其监控项构成则为 `算子类型_算子信息_算子索引_具体监控项`。

以 `source_demo_0_records_in_total` 为例，其中 `source` 代表了读数据算子，`demo` 为对应的 stream，`0` 代表了该算子实例在并发度中的索引，而 `records_in_total` 则诠释了实际的监控项，即该算子接收了多少条记录。

当我们尝试往该 stream 发送了一条数据后，再次获取该规则状态如下:

```json
{
  "status": "running",
  "source_demo_0_records_in_total": 1,
  "source_demo_0_records_out_total": 1,
  ......
  "op_2_project_0_records_in_total": 1,
  "op_2_project_0_records_out_total": 1,
  ......
  "sink_mqtt_0_0_records_in_total": 1,
  "sink_mqtt_0_0_records_out_total": 1,
  ......
}
```

可以看到每个算子的 `records_in_total` 与 `records_out_total` 都由 0 变为了 1，代表了该算子接收到了一条记录，并向下一个算子传递了一条记录，最终在 `sink` 端向 sink 写入了 1 条记录。
