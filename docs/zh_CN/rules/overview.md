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

- Kuiper内置支持以下 3 种源
    - MQTT 源，有关更多详细信息，请参阅[MQTT source stream](sources/mqtt.md)。
    - EdgeX 源缺省是包含在[容器镜像](https://hub.docker.com/r/emqx/kuiper)中发布的，但是没有包含在单独下载的二进制包中，您可以使用 ``make pkg_with_edgex`` 命令来编译出一个支持 EdgeX 源的程序。更多关于它的详细信息，请参考 [EdgeX source stream](sources/edgex.md)。
    - HTTP 定时拉取源，按照用户指定的时间间隔，定时从 HTTP 服务器中拉取数据，更多详细信息，请参考[这里](sources/http_pull.md) 。 
- 有关Kuiper SQL的更多信息，请参阅[SQL](../sqls/overview.md)。
- 可以自定义来源，请参阅 [extension](../extension/overview.md)了解更多详细信息。

### 动作

当前，支持以下操作： [log](sinks/logs.md) 、[mqtt](sinks/mqtt.md) 、[edgex](sinks/edgex.md)、[rest](sinks/rest.md) 和 [nop](sinks/nop.md)。 每个动作可以定义自己的属性。当前有以下的公共属性:

| 属性名 | 类型和默认值 | 描述                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| concurrency | int: 1   | 设置运行的线程数。该参数值大于1时，消息发出的顺序可能无法保证。 |
| bufferLength | int: 1024   | 设置可缓存消息数目。若缓存消息数超过此限制，sink将阻塞消息接收，直到缓存消息被消费使得缓存消息数目小于限制为止。|
| runAsync        | bool:false   | 设置是否异步运行输出操作以提升性能。请注意，异步运行的情况下，输出结果顺序不能保证。  |
| retryInterval   | int:1000   | 设置信息发送失败后重试等待时间，单位为毫秒。如果该值的设置 <= 0，那么不会尝试重新发送。 |
| cacheLength     | int:10240   | 设置最大消息缓存数量。缓存的消息会一直保留直到消息发送成功。缓存消息将按顺序发送，除非运行在异步或者并发模式下。缓存消息会定期存储到磁盘中。  |
| cacheSaveInterval  | int:1000   | 设置缓存存储间隔时间，单位为毫秒。需要注意的是，当规则关闭时，缓存会自动存储。该值越大，则缓存保存开销越小，但系统意外退出时缓存丢失的风险变大。 |
| omitIfEmpty | bool: false | 如果选择结果为空，则忽略输出。 |
| sendSingle        | true     | 输出消息以数组形式接收，该属性意味着是否将结果一一发送。 如果为false，则输出消息将为``{"result":"${接收消息字符串}"}``。 例如，``{"result":"[{\"count\":30},"\"count\":20}]"}``。否则，结果消息将与实际字段名称一一对应发送。 对于与上述相同的示例，它将发送``{"count":30}``，然后发送``{"count":20}``到RESTful端点。默认为false。 |
| dataTemplate      | true     | [golang模板](https://golang.org/pkg/html/template)格式字符串，用于指定输出数据格式。 模板的输入是目标消息，该消息始终是映射数组。 如果未指定数据模板，则将数据作为原始输入。 |

#### 数据模板
如果sendSingle为true，则数据模板将针对某一条记录执行操作； 否则，它将对整个记录数组执行操作。 典型的数据模板是：

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

- 打印ab字段

```
"dataTemplate": "{\"content\":{{.ab}}}",
```

如果ab字段是字符串，请添加引号
```
"dataTemplate": "{\"content\":\"{{.ab}}\"}",
```

在sendSingle=false模式下：
- 打印出整个记录数组

```
"dataTemplate": "{\"content\":{{json .}}}",
```

- 打印出第一条记录

```
"dataTemplate": "{\"content\":{{json (index . 0)}}}",
```

- 打印出第一个记录的字段ab

```
"dataTemplate": "{\"content\":{{index . 0 \"ab\"}}}",
```

- 将数组中每个记录的字段ab打印为html格式

```
"dataTemplate": "<div>results</div><ul>{{range .}}<li>{{.ab}}</li>{{end}}</ul>",
```


可以自定义动作以支持不同种类的输出，有关更多详细信息，请参见 [extension](../extension/overview.md) 。

### 选项
当前的选项包括：

| 选项名 | 类型和默认值 | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| isEventTime | 布尔值：false | 使用事件时间还是将时间用作事件的时间戳。 如果使用事件时间，则将从有效负载中提取时间戳。 必须通过[stream]([extension](../sqls/streams.md))定义指定时间戳记。 |
| lateTolerance        | int64:0   | 在使用事件时间窗口时，可能会出现元素延迟到达的情况。 LateTolerance可以指定在删除元素之前可以延迟多少时间（单位为毫秒）。 默认情况下，该值为0，表示后期元素将被删除。 |
| concurrency | int: 1   | 一条规则运行时会根据sql语句分解成多个plan运行。该参数设置每个plan运行的线程数。该参数值大于1时，消息处理顺序可能无法保证。 |
| bufferLength | int: 1024   | 指定每个plan可缓存消息数。若缓存消息数超过此限制，plan将阻塞消息接收，直到缓存消息被消费使得缓存消息数目小于限制为止。此选项值越大，则消息吞吐能力越强，但是内存占用也会越多。|
