# Sink

在 eKuiper 源代码中，有内置的动作和扩展的动作。

## 内置动作

用户可以直接使用标准 eKuiper 实例中的内置动作。内建动作的列表如下。

- [Mqtt sink](./builtin/mqtt.md)：输出到外部 mqtt 服务。
- [Neuron sink](./builtin/neuron.md)：输出到本地的 Neuron 实例。
- [EdgeX sink](./builtin/edgex.md)：输出到 EdgeX Foundry。此动作仅在启用 edgex 编译标签时存在。
- [Rest sink](./builtin/rest.md)：输出到外部 http 服务器。
- [Redis sink](./builtin/redis.md): 写入 Redis 。
- [File sink](./builtin/file.md)： 写入文件。
- [Memory sink](./builtin/memory.md)：输出到 eKuiper 内存主题以形成规则管道。
- [Log sink](./builtin/log.md)：写入日志，通常只用于调试。
- [Nop sink](./builtin/nop.md)：不输出，用于性能测试。

## 预定义的动作插件

我们已经开发了一些官方的动作插件。这些插件可以在 eKuiper 的源代码中找到，用户需要手动构建它们。详细信息请查看每个动作的构建和使用方法。

这些插件有预编译的二进制文件，用于主流cpu架构，如AMD或ARM。预编译的插件托管在 `https：//packages.emqx.net/kuiper-plugins/$version/$os/sinks/$type_$arch.zip` 中。例如，要获得用于 debian amd64 的 tdengine 插件，请从 `https：//packages.emqx.net/kuiper-plugins/1.4.4/debian/sinks/tdengine_amd64.zip` 安装。

预定义的动作插件列表。

- [InfluxDB sink](./plugin/influx.md)： 写入 Influx DB `v1.x`。
- [InfluxDBV2 sink](./plugin/influx2.md)： 写入 Influx DB `v2.x`。
- [Tdengine sink](./plugin/tdengine.md)： 写入 Tdengine 。
- [Image sink](./plugin/image.md)： 写入一个图像文件。仅用于处理二进制结果。
- [Zero MQ sink](./plugin/zmq.md)：输出到 Zero MQ 。
- [Kafka sink](./plugin/kafka.md)：输出到 Kafka 。

## 更新

默认情况下，Sink 将数据附加到外部系统中。一些外部系统，如 SQL DB 本身是可更新的，允许更新或删除数据。与查找源类似，只有少数 Sink 是天然 "可更新 "的。可更新的 Sink 必须支持插入、更新和删除。产品自带的 Sink 种，可更新的包括: 

- Memory Sink
- Redis Sink
- SQL Sink

为了激活更新功能，Sink 必须设置 `rowkindField` 属性，以指定数据中的哪个字段代表要采取的动作。在下面的例子中，`rowkindField` 被设置为 `action`。

```json
{"redis": {
  "addr": "127.0.0.1:6379",
  "dataType": "string",
  "field": "id",
  "rowkindField": "action",
  "sendSingle": true
}}
```

流入的数据必须有一个字段来表示更新的动作。在下面的例子中，`action` 字段是要执行的动作。动作可以是插入、更新、 upsert 和删除。不同的 sink 的动作实现是不同的。有些 sink 可能对插入、upsert 和更新执行相同的动作。

```json
{"action":"update", "id":5, "name":"abc"}
```

这条信息将把id 为 5的数据更新为新的名字。

## 公共属性

每个 sink 都有基于共同的属性的属性集。

每个动作可以定义自己的属性。当前有以下的公共属性:

| 属性名                  | 类型和默认值                          | 描述                                                                                                                                                                                                                                         |
|----------------------|---------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| concurrency          | int: 1                          | 设置运行的线程数。该参数值大于1时，消息发出的顺序可能无法保证。                                                                                                                                                                                                           |
| bufferLength         | int: 1024                       | 设置可缓存消息数目。若缓存消息数超过此限制，sink将阻塞消息接收，直到缓存消息被消费使得缓存消息数目小于限制为止。                                                                                                                                                                                 |
| runAsync(deprecated) | bool:false                      | 设置是否异步运行输出操作以提升性能。请注意，异步运行的情况下，输出结果顺序不能保证。                                                                                                                                                                                                 |
| omitIfEmpty          | bool: false                     | 如果配置项设置为 true，则当 SELECT 结果为空时，该结果将不提供给目标运算符。                                                                                                                                                                                               |
| sendSingle           | bool: false                     | 输出消息以数组形式接收，该属性意味着是否将结果一一发送。 如果为false，则输出消息将为`{"result":"${the string of received message}"}`。 例如，`{"result":"[{\"count\":30},"\"count\":20}]"}`。否则，结果消息将与实际字段名称一一对应发送。 对于与上述相同的示例，它将发送 `{"count":30}`，然后发送`{"count":20}`到 RESTful 端点。默认为 false。 |
| dataTemplate         | string: ""                      | [golang 模板](https://golang.org/pkg/html/template)格式字符串，用于指定输出数据格式。 模板的输入是目标消息，该消息始终是映射数组。 如果未指定数据模板，则将数据作为原始输入。                                                                                                                            |
| format               | string: "json"                  | 编码格式，支持 "json" 和 "protobuf"。若使用 "protobuf", 需通过 "schemaId" 参数设置模式，并确保模式已注册。                                                                                                                                                                |
| schemaId             | string: ""                      | 编码使用的模式。                                                                                                                                                                                                                                   |
| delimiter            | string: ","                     | 仅在使用 `delimited` 格式时生效，用于指定分隔符，默认为逗号。                                                                                                                                                                                                      |
| fields               | []string: nil                   | 用于选择输出消息的字段。例如，sql查询的结果是`{"temperature": 31.2, humidity": 45}`， fields为`["humidity"]`，那么最终输出为`{"humidity": 45}`。建议不要同时配置`dataTemplate`和`fields`。如果同时配置，先根据`dataTemplate`得到输出数据，再通过`fields`得到最终结果。                                          |
| dataField            | string: ""                        | 用于选择 dataField 的嵌套值。                                                                                                                                                                                                                       |
| enableCache          | bool: 默认值为`etc/kuiper.yaml` 中的全局配置 | 是否启用sink cache。缓存存储配置遵循 `etc/kuiper.yaml` 中定义的元数据存储的配置。                                                                                                                                                                                    |
| memoryCacheThreshold | int: 默认值为全局配置                   | 要缓存在内存中的消息数量。出于性能方面的考虑，最早的缓存信息被存储在内存中，以便在故障恢复时立即重新发送。这里的数据会因为断电等故障而丢失。                                                                                                                                                                     |
| maxDiskCache         | int: 默认值为全局配置                   | 缓存在磁盘中的信息的最大数量。磁盘缓存是先进先出的。如果磁盘缓存满了，最早的一页信息将被加载到内存缓存中，取代旧的内存缓存。                                                                                                                                                                             |
| bufferPageSize       | int: 默认值为全局配置                   | 缓冲页是批量读/写到磁盘的单位，以防止频繁的IO。如果页面未满，eKuiper 因硬件或软件错误而崩溃，最后未写入磁盘的页面将被丢失。                                                                                                                                                                        |
| resendInterval       | int: 默认值为全局配置                   | 故障恢复后重新发送信息的时间间隔，防止信息风暴。                                                                                                                                                                                                                   |
| cleanCacheAtStop     | bool: 默认值为全局配置                  | 是否在规则停止时清理所有缓存，以防止规则重新启动时对过期消息进行大量重发。如果不设置为true，一旦规则停止，内存缓存将被存储到磁盘中。否则，内存和磁盘规则会被清理掉。                                                                                                                                                       |


### 动态属性

有些情况下，用户需要按照数据把结果发送到不同的目标中。例如，根据收到的数据，把计算结果发到不同的 mqtt 主题中。使用基于[数据模板](./data_template.md)格式的动态属性，可以实现这样的功能。在以下的例子中，目标的 topic 属性是一个数据模板格式的字符串从而在运行时会将消息发送到动态的主题中。

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

## 缓存

动作用于将处理结果发送到外部系统中，存在外部系统不可用的情况，特别是在从边到云的场景中。例如，在弱网情况下，边到云的网络连接可能会不时断开和重连。因此，动作提供了缓存功能，用于在发送错误的情况下暂存数据，并在错误恢复之后自动重发缓存数据。动作的缓存可分为内存和磁盘的两级存储。用户可配置内存缓存条数，超过上限后，新的缓存将离线存储到磁盘中。缓存将同时保存在内存和磁盘中，这样缓存的容量就变得更大了；它还将持续检测故障恢复状态，并在不重新启动规则的情况下重新发送。

离线缓存的保存位置根据 `etc/kuiper.yaml` 里的 store 配置决定，默认为 sqlite 。如果磁盘存储是sqlite，所有的缓存将被保存到`data/cache.db`文件。每个 sink 将有一个唯一的 sqlite 表来保存缓存。缓存的计数添加到 sink 的 指标中的 buffer length 部分。

### 流程

每个sink都可以配置自己的缓存机制。每个 sink 的缓存流程是相同的。如果启用了缓存，所有 sink 的事件都会经过两个阶段：首先是将所有内容保存到缓存中；然后在收到ack后删除缓存。

- 错误检测：发送失败后，sink应该通过返回特定的错误类型来识别可恢复的失败（网络等），这将返回一个失败的ack，这样缓存就可以被保留下来。对于成功的发送或不可恢复的错误，将发送一个成功的 ack 来删除缓存。
- 缓存机制：缓存将首先被保存在内存中。如果超过了内存的阈值，后面的缓存将被保存到磁盘中。一旦磁盘缓存超过磁盘存储阈值，缓存将开始rotate，即内存中最早的缓存将被丢弃，并加载磁盘中最早的缓存来代替。
- 重发策略：目前缓存机制仅可运行在默认的同步模式中，如果有一条消息正在发送中，则会等待发送的结果以继续发送下个缓存数据。否则，当有新的数据到来时，发送缓存中的第一个数据以检测网络状况。如果发送成功，将按顺序链式发送所有内存和磁盘中的所有缓存。链式发送可定义一个发送间隔，防止形成消息风暴。

### 配置

Sink 缓存的配置有两个层次。`etc/kuiper.yaml` 中的全局配置，定义所有规则的默认行为。还有一个规则 sink 层的定义，用来覆盖默认行为。

- enableCache：是否启用sink cache。缓存存储配置遵循 `etc/kuiper.yaml` 中定义的元数据存储的配置。
- memoryCacheThreshold：要缓存在内存中的消息数量。出于性能方面的考虑，最早的缓存信息被存储在内存中，以便在故障恢复时立即重新发送。这里的数据会因为断电等故障而丢失。
- maxDiskCache: 缓存在磁盘中的信息的最大数量。磁盘缓存是先进先出的。如果磁盘缓存满了，最早的一页信息将被加载到内存缓存中，取代旧的内存缓存。
- bufferPageSize。缓冲页是批量读/写到磁盘的单位，以防止频繁的IO。如果页面未满，eKuiper 因硬件或软件错误而崩溃，最后未写入磁盘的页面将被丢失。
- resendInterval: 故障恢复后重新发送信息的时间间隔，防止信息风暴。
- cleanCacheAtStop：是否在规则停止时清理所有缓存，以防止规则重新启动时对过期消息进行大量重发。如果不设置为true，一旦规则停止，内存缓存将被存储到磁盘中。否则，内存和磁盘规则会被清理掉。

在以下规则的示例配置中，log sink 没有配置缓存相关选项，因此将会采用全局默认配置；而 mqtt sink 进行了自身缓存策略的配置。

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [{
    "log": {},
    "mqtt": {
      "server": "tcp://127.0.0.1:1883",
      "topic": "result/cache",
      "qos": 0,
      "enableCache": true,
      "memoryCacheThreshold": 2048,
      "maxDiskCache": 204800,
      "bufferPageSize": 512,
      "resendInterval": 10
    }
  }]
}
```

## 资源引用
 
像源一样，动作也支持配置复用，用户只需要在 sinks 文件夹中创建与目标动作同名的 yaml 文件并按照源一样的形式写入配置。

例如，针对 MQTT 动作场景， 用户可以在 sinks 目录下创建 mqtt.yaml 文件，并写入如下内容
```yaml
test:
  qos: 1
  server: "tcp://broker.emqx.io:1883"
```
当用户需要 MQTT 动作时，除了采用传统的配置方式，如下所示
```json
    {
      "mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "devices/demo_001/messages/events/",
        "protocolVersion": "3.1.1",
        "qos": 1,
        "clientId": "demo_001",
        "username": "xyz.azure-devices.net/demo_001/?api-version=2018-06-30",
        "password": "SharedAccessSignature sr=*******************",
        "retained": false
      }
    }
```
还可以通过 `resourceId` 引用形式，采用如下的配置

```json
 {
      "mqtt": {
        "resourceId": "test",
        "topic": "devices/demo_001/messages/events/",
        "protocolVersion": "3.1.1",
        "clientId": "demo_001",
        "username": "xyz.azure-devices.net/demo_001/?api-version=2018-06-30",
        "password": "SharedAccessSignature sr=*******************",
        "retained": false
      }
}
```