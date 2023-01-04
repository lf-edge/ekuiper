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

有两种方法来定义规则的业务逻辑。要么使用SQL/动作组合，要么使用新增加的图API。

### SQL 规则

通过指定 `sql` 和 `actions` 属性，我们可以以声明的方式定义规则的业务逻辑。其中，`sql` 定义了针对预定义流运行的 SQL 查询，这将转换数据。然后，输出的数据可以通过 `action` 路由到多个位置。参见[SQL](../sqls/overview.md)了解更多eKuiper SQL的信息。

#### 源

- eKuiper 支持以下内置源：
  - MQTT 源，有关更多详细信息，请参阅 [MQTT source stream](https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/rules/sources/mqtt.md)。
  - EdgeX 源缺省是包含在[容器镜像](https://hub.docker.com/r/lfedge/ekuiper)中发布的，但是没有包含在单独下载的二进制包中，您可以使用 `make pkg_with_edgex` 命令来编译出一个支持 EdgeX 源的程序。更多关于它的详细信息，请参考 [EdgeX source stream](https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/rules/sources/edgex.md)。
  - HTTP 定时拉取源，按照用户指定的时间间隔，定时从 HTTP 服务器中拉取数据，更多详细信息，请参考[这里](https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/rules/sources/http_pull.md) 。
- 可以自定义来源，请参阅 [extension](https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/extension/overview.md)了解更多详细信息。

#### 目标/动作

当前，支持以下目标/动作：

- [log](./sinks/builtin/log.md): 将结果发送到日志文件。
- [mqtt](./sinks/builtin/mqtt.md): 将结果发送到 MQTT 消息服务器。
- [edgex](./sinks/builtin/edgex.md): 将结果发送到 EdgeX 消息总线。
- [rest](./sinks/builtin/rest.md): 将结果发送到 Rest HTTP 服务器。
- [nop](./sinks/builtin/nop.md): 将结果发送到 nop 操作。

每个动作可以定义自己的属性。当前有以下的公共属性:

| 属性名                  | 类型和默认值                             | 描述                                                                                                                                                                                                                                               |
|----------------------|------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| concurrency          | int: 1                             | 设置运行的线程数。该参数值大于1时，消息发出的顺序可能无法保证。                                                                                                                                                                                                                 |
| bufferLength         | int: 1024                          | 设置可缓存消息数目。若缓存消息数超过此限制，sink将阻塞消息接收，直到缓存消息被消费使得缓存消息数目小于限制为止。                                                                                                                                                                                       |
| runAsync             | bool:false                         | 设置是否异步运行输出操作以提升性能。请注意，异步运行的情况下，输出结果顺序不能保证。                                                                                                                                                                                                       |
| omitIfEmpty          | bool: false                        | 如果配置项设置为 true，则当 SELECT 结果为空时，该结果将不提供给目标运算符。                                                                                                                                                                                                     |
| sendSingle           | bool: false                        | 输出消息以数组形式接收，该属性意味着是否将结果一一发送。 如果为false，则输出消息将为`{"result":"${the string of received message}"}`。 例如，`{"result":"[{\"count\":30},"\"count\":20}]"}`。否则，结果消息将与实际字段名称一一对应发送。 对于与上述相同的示例，它将发送 `{"count":30}`，然后发送`{"count":20}`到 RESTful 端点。默认为 false。 |
| dataTemplate         | string: ""                         | [golang 模板](https://golang.org/pkg/html/template)格式字符串，用于指定输出数据格式。 模板的输入是目标消息，该消息始终是映射数组。 如果未指定数据模板，则将数据作为原始输入。                                                                                                                                  |
| format               | string: "json"                     | 编码格式，支持 "json" 和 "protobuf"。若使用 "protobuf", 需通过 "schemaId" 参数设置模式，并确保模式已注册。                                                                                                                                                                      |
| schemaId             | string: ""                         | 编码使用的模式。                                                                                                                                                                                                                                         |
| delimiter            | string: ","                        | 仅在使用 `delimited` 格式时生效，用于指定分隔符，默认为逗号。                                                                                                                                                                                                            |
| enableCache          | bool: 默认值为`etc/kuiper.yaml` 中的全局配置 | 是否启用sink cache。缓存存储配置遵循 `etc/kuiper.yaml` 中定义的元数据存储的配置。                                                                                                                                                                                          |
| memoryCacheThreshold | int: 默认值为全局配置                      | 要缓存在内存中的消息数量。出于性能方面的考虑，最早的缓存信息被存储在内存中，以便在故障恢复时立即重新发送。这里的数据会因为断电等故障而丢失。                                                                                                                                                                           |
| maxDiskCache         | int: 默认值为全局配置                      | 缓存在磁盘中的信息的最大数量。磁盘缓存是先进先出的。如果磁盘缓存满了，最早的一页信息将被加载到内存缓存中，取代旧的内存缓存。                                                                                                                                                                                   |
| bufferPageSize       | int: 默认值为全局配置                      | 缓冲页是批量读/写到磁盘的单位，以防止频繁的IO。如果页面未满，eKuiper 因硬件或软件错误而崩溃，最后未写入磁盘的页面将被丢失。                                                                                                                                                                              |
| resendInterval       | int: 默认值为全局配置                      | 故障恢复后重新发送信息的时间间隔，防止信息风暴。                                                                                                                                                                                                                         |
| cleanCacheAtStop     | bool: 默认值为全局配置                     | 是否在规则停止时清理所有缓存，以防止规则重新启动时对过期消息进行大量重发。如果不设置为true，一旦规则停止，内存缓存将被存储到磁盘中。否则，内存和磁盘规则会被清理掉。                                                                                                                                                             |

##### 数据模板

用户可以参考 [eKuiper 中使用 Golang 模版 (template) 定制分析结果](./data_template.md) 来获取更多的关于数据模版的使用场景。

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

###### 模版中支持的函数

用户可通过模板函数，对数据进行各种转换，包括但不限于格式转换，数学计算和编码等。eKuiper 中支持的模板函数包括以下几类：

1. Go 语言内置[模板函数](https://golang.org/pkg/text/template/#hdr-Functions)。
2. 来自 [sprig library](http://masterminds.github.io/sprig/) 的丰富的扩展函数集。
3. eKuiper 扩展的函数。

eKuiper 扩展了几个可以在模版中使用的函数。

- (deprecated)`json para1`: `json` 函数用于将 map 内容转换为 JSON 字符串。本函数已弃用，建议使用 sprig 扩展的 `toJson` 函数。
- (deprecated)`base64 para1`: `base64` 函数用于将参数值编码为 base64 字符串。本函数已弃用，建议将参数转换为 string 类型后，使用 sprig 扩展的 `b64enc` 函数。

##### 动态属性

有些情况下，用户需要按照数据把结果发送到不同的目标中。例如，根据收到的数据，把计算结果发到不同的 mqtt 主题中。使用基于[数据模板](#数据模板)格式的动态属性，可以实现这样的功能。在以下的例子中，目标的 topic 属性是一个数据模板格式的字符串从而在运行时会将消息发送到动态的主题中。

```json
{
  "id": "rule1",
  "sql": "SELECT topic FROM demo",
  "actions": [{
    "mqtt": {
      "sendSingle": true,
      "topic": "prefix/{{.topic}}"
    }
  }]
}
```

需要注意的是，上例中的 `sendSingle` 属性已设置。在默认情况下，目标接收到的是数组，使用的 jsonpath 需要采用 <code v-pre>{{index . 0 "topic"}}</code>。

#### 编解码

规则的 source 读入事件的时候，需要将来自不同类型源的各种类型的数据，解析并解码为内部处理的 Map 类型数据。规则计算结束之后，sink 需要把内部的 Map 类型数据，编码成各种类型外部系统的专有格式。在我们支持的 source/sink 中主要有两种类型，一种是连接的外部系统有固定的私有格式，因此在 source/sink 的实现中已经包含了编解码的工作，例如 EdgeX，Neuron；另一种是连接的外部系统仅规定了连接的协议，而传输的数据允许自定义格式，例如 MQTT, ZeroMQ。后一种类型的 source/sink，可通过配置 `format` 和 `schemaId` 参数，实现灵活的编解码方案。关于支持的编解码格式和模式的管理，请参考[编解码](./codecs.md)。

### 图规则

在 1.6.0 及之后的版本中，eKuiper 在规则模型中提供了图形属性作为创建规则的另一种方式。该属性以 JSON 格式定义了一个规则的有向无环图。它很容易直接映射到可视化编辑器中的图形，并适合作为拖放用户界面的后端。下面是一个图形规则定义的例子。

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

`graph` 属性是一个json结构，其中 `nodes` 用来定义图形中呈现的节点，`topo` 用来定义节点之间的连线。节点类型可以是内置的节点类型，如 window 节点和 filter 节点等。它也可以是来自插件的用户定义的节点。更多细节请参考[图规则](graph_rule.md)。

## 选项

当前的选项包括：

| 选项名                | 类型和默认值     | 说明                                                                                             |
|--------------------|------------|------------------------------------------------------------------------------------------------|
| isEventTime        | bool:false | 使用事件时间还是将时间用作事件的时间戳。 如果使用事件时间，则将从有效负载中提取时间戳。 必须通过 [stream](../sqls/streams.md) 定义指定时间戳记。       |
| lateTolerance      | int64:0    | 在使用事件时间窗口时，可能会出现元素延迟到达的情况。 LateTolerance 可以指定在删除元素之前可以延迟多少时间（单位为 ms）。 默认情况下，该值为0，表示后期元素将被删除。   |
| concurrency        | int: 1     | 一条规则运行时会根据 sql 语句分解成多个 plan 运行。该参数设置每个 plan 运行的线程数。该参数值大于1时，消息处理顺序可能无法保证。                      |
| bufferLength       | int: 1024  | 指定每个 plan 可缓存消息数。若缓存消息数超过此限制，plan 将阻塞消息接收，直到缓存消息被消费使得缓存消息数目小于限制为止。此选项值越大，则消息吞吐能力越强，但是内存占用也会越多。 |
| sendMetaToSink     | bool:false | 指定是否将事件的元数据发送到目标。 如果为 true，则目标可以获取元数据信息。                                                       |
| sendError          | bool: true | 指定是否将运行时错误发送到目标。如果为 true，则错误会在整个流中传递直到目标。否则，错误会被忽略，仅打印到日志中。                                    |
| qos                | int:0      | 指定流的 qos。 值为0对应最多一次； 1对应至少一次，2对应恰好一次。 如果 qos 大于0，将激活检查点机制以定期保存状态，以便可以从错误中恢复规则。                 |
| checkpointInterval | int:300000 | 指定触发检查点的时间间隔（单位为 ms）。 仅当 qos 大于0时才有效。                                                          |
| restartStrategy    | 结构         | 指定规则运行失败后自动重新启动规则的策略。这可以帮助从可恢复的故障中回复，而无需手动操作。请查看[规则重启策略](#规则重启策略)了解详细的配置项目。                    |

有关 `qos` 和 `checkpointInterval` 的详细信息，请查看[状态和容错](./state_and_fault_tolerance.md)。

可以在 `rules` 下属的 `etc/kuiper.yaml` 中全局定义规则选项。 规则 json 中定义的选项将覆盖全局设置。

### 规则重启策略

规则重启策略的配置项包括：

| 选项名          | 类型和默认值     | 说明                                                        |
|--------------|------------|-----------------------------------------------------------|
| attempts     | int: 0     | 最大重试次数。如果设置为0，该规则将立即失败，不会进行重试。                            |
| delay        | int: 1000  | 默认的重试间隔时间，以毫秒为单位。如果没有设置`multiplier`，重试的时间间隔将固定为这个值。       |
| maxDelay     | int: 30000 | 重试的最大间隔时间，单位是毫秒。只有当`multiplier`有设置时，从而使得每次重试的延迟都会增加时才会生效。 |
| multiplier   | float: 2   | 重试间隔时间的乘数。                                                |
| jitterFactor | float: 0.1 | 添加或减去延迟的随机值系数，防止在同一时间重新启动多个规则。                            |

这些选项的默认值定义于 `etc/kuiper.yaml` 配置文件，可通过修改该文件更改默认值。