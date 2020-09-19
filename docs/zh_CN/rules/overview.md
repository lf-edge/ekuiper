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

创建规则需要以下3个参数。

## 参数

| 参数名 | 是否可选 | 说明                |
| ------------- | -------- | ------------------------------------------------------------ |
| id | 否  | 规则 id |
| sql        | 否  | 为规则运行的 sql 查询 |
| actions           | 否   | Sink 动作数组 |
| options           | 是       | 选项图     |

## id

规则的标识。 规则名称不能在同一 Kuiper 实例中重复。

## sql

为规则运行的 sql 查询。

### 选项

当前的选项包括：

| 选项名             | 类型和默认值 | 说明                                                         |
| ------------------ | ------------ | ------------------------------------------------------------ |
| isEventTime        | bool:false   | 使用事件时间还是将时间用作事件的时间戳。 如果使用事件时间，则将从有效负载中提取时间戳。 必须通过 [stream]([extension](../sqls/streams.md)) 定义指定时间戳记。 |
| lateTolerance      | int64:0      | 在使用事件时间窗口时，可能会出现元素延迟到达的情况。 LateTolerance 可以指定在删除元素之前可以延迟多少时间（单位为 ms）。 默认情况下，该值为0，表示后期元素将被删除。 |
| concurrency        | int: 1       | 一条规则运行时会根据 sql 语句分解成多个 plan 运行。该参数设置每个 plan 运行的线程数。该参数值大于1时，消息处理顺序可能无法保证。 |
| bufferLength       | int: 1024    | 指定每个 plan 可缓存消息数。若缓存消息数超过此限制，plan 将阻塞消息接收，直到缓存消息被消费使得缓存消息数目小于限制为止。此选项值越大，则消息吞吐能力越强，但是内存占用也会越多。 |
| sendMetaToSink     | bool:false   | 指定是否将事件的元数据发送到目标。 如果为 true，则目标可以获取元数据信息。 |
| qos                | int:0        | 指定流的 qos。 值为0对应最多一次； 1对应至少一次，2对应恰好一次。 如果 qos 大于0，将激活检查点机制以定期保存状态，以便可以从错误中恢复规则。 |
| checkpointInterval | int:300000   | 指定触发检查点的时间间隔（单位为 ms）。 仅当 qos 大于0时才有效。 |

有关 `qos` 和 `checkpointInterval` 的详细信息，请查看[状态和容错](state_and_fault_tolerance)。

可以在 `rules` 下属的 `etc/kuiper.yaml` 中全局定义规则选项。 规则 json 中定义的选项将覆盖全局设置。

## 源

- Kuiper 支持以下 3 种内置源：
  - MQTT 源，有关更多详细信息，请参阅 [MQTT source stream](https://github.com/emqx/kuiper/blob/dev/0.9.1/docs/zh_CN/rules/sources/mqtt.md)。
  - EdgeX 源缺省是包含在[容器镜像](https://hub.docker.com/r/emqx/kuiper)中发布的，但是没有包含在单独下载的二进制包中，您可以使用 `make pkg_with_edgex` 命令来编译出一个支持 EdgeX 源的程序。更多关于它的详细信息，请参考 [EdgeX source stream](https://github.com/emqx/kuiper/blob/dev/0.9.1/docs/zh_CN/rules/sources/edgex.md)。
  - HTTP 定时拉取源，按照用户指定的时间间隔，定时从 HTTP 服务器中拉取数据，更多详细信息，请参考[这里](https://github.com/emqx/kuiper/blob/dev/0.9.1/docs/zh_CN/rules/sources/http_pull.md) 。
- 有关Kuiper SQL 的更多信息，请参阅 [SQL](https://github.com/emqx/kuiper/blob/dev/0.9.1/docs/zh_CN/sqls/overview.md)。
- 可以自定义来源，请参阅 [extension](https://github.com/emqx/kuiper/blob/dev/0.9.1/docs/zh_CN/extension/overview.md)了解更多详细信息。

### 目标/动作

当前，支持以下目标/动作：

- [log](sinks/logs.md): 将结果发送到日志文件。
- [mqtt](sinks/mqtt.md): 将结果发送到 MQTT 消息服务器。 
- [edgex](sinks/edgex.md): 将结果发送到 EdgeX 消息总线。
- [rest](sinks/rest.md): 将结果发送到 Rest HTTP 服务器。
- [nop](sinks/nop.md): 将结果发送到 nop 操作。

每个动作可以定义自己的属性。当前有以下的公共属性:

| 属性名 | 类型和默认值 | 描述                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| concurrency | int: 1   | 设置运行的线程数。该参数值大于1时，消息发出的顺序可能无法保证。 |
| bufferLength | int: 1024   | 设置可缓存消息数目。若缓存消息数超过此限制，sink将阻塞消息接收，直到缓存消息被消费使得缓存消息数目小于限制为止。|
| runAsync        | bool:false   | 设置是否异步运行输出操作以提升性能。请注意，异步运行的情况下，输出结果顺序不能保证。  |
| retryInterval   | int:1000   | 设置信息发送失败后重试等待时间，单位为毫秒。如果该值的设置 <= 0，那么不会尝试重新发送。 |
| cacheLength     | int:1024   | 设置最大消息缓存数量。缓存的消息会一直保留直到消息发送成功。缓存消息将按顺序发送，除非运行在异步或者并发模式下。缓存消息会定期存储到磁盘中。  |
| cacheSaveInterval  | int:1000   | 设置缓存存储间隔时间。需要注意的是，当规则关闭时，缓存会自动存储。该值越大，则缓存保存开销越小，但系统意外退出时缓存丢失的风险变大。 |
| omitIfEmpty | bool: false | 如果配置项设置为 true，则当 SELECT 结果为空时，该结果将不提供给目标运算符。 |
| sendSingle        | true     | 输出消息以数组形式接收，该属性意味着是否将结果一一发送。 如果为false，则输出消息将为`{"result":"${the string of received message}"}`。 例如，`{"result":"[{\"count\":30},"\"count\":20}]"}`。否则，结果消息将与实际字段名称一一对应发送。 对于与上述相同的示例，它将发送 `{"count":30}`，然后发送`{"count":20} `到 RESTful 端点。默认为 false。 |
| dataTemplate      | true     | [golang 模板](https://golang.org/pkg/html/template)格式字符串，用于指定输出数据格式。 模板的输入是目标消息，该消息始终是映射数组。 如果未指定数据模板，则将数据作为原始输入。 |

#### 数据模板
用户可以参考 [Kuiper 中使用 Golang 模版 (template) 定制分析结果](data_template.md) 来获取更多的关于数据模版的使用场景。

如果 sendSingle 为 true，则数据模板将针对某一条记录执行操作； 否则，它将对整个记录数组执行操作。 典型的数据模板是：

例如，我们的目标输入为

```
[]map[string]interface{}{{
    "ab" : "hello1",
},{
    "ab" : "hello2",
}}
```

在 sendSingle=true 模式下：
- 打印整个记录

```
"dataTemplate": "{\"content\":{{json .}}}",
```

- 打印 ab 字段

```
"dataTemplate": "{\"content\":{{.ab}}}",
```

如果 ab 字段是字符串，请添加引号
```
"dataTemplate": "{\"content\":\"{{.ab}}\"}",
```

在 sendSingle=false 模式下：
- 打印出整个记录数组

```
"dataTemplate": "{\"content\":{{json .}}}",
```

- 打印出第一条记录

```
"dataTemplate": "{\"content\":{{json (index . 0)}}}",
```

- 打印出第一个记录的字段 ab

```
"dataTemplate": "{\"content\":{{index . 0 \"ab\"}}}",
```

- 将数组中每个记录的字段 ab 打印为 html 格式

```
"dataTemplate": "<div>results</div><ul>{{range .}}<li>{{.ab}}</li>{{end}}</ul>",
```


可以自定义动作以支持不同种类的输出，有关更多详细信息，请参见 [extension](../extension/overview.md) 。

#### 模版中支持的函数

Kuiper 扩展了几个可以在模版中使用的函数。

- `json para1`: `json` 函数用于将 map 内容转换为 JSON 字符串
- `base64 para1`: `base64` 函数用于将参数值编码为 base64 字符串
- `add para1 para2`: `add` 函数用于将两个数值类型的参数相加
