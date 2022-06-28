# 可用的动作

在 eKuiper 源代码中，有内置的动作和扩展的动作。

## 内置动作

用户可以直接使用标准 eKuiper 实例中的内置动作。内建动作的列表如下。

- [Mqtt sink](./builtin/mqtt.md)：输出到外部 mqtt 服务。
- [Neuron sink](./builtin/neuron.md)：输出到本地的 Neuron 实例。
- [EdgeX sink](./builtin/edgex.md)：输出到 EdgeX Foundry。此动作仅在启用 edgex 编译标签时存在。
- [Rest sink](./builtin/rest.md)：输出到外部 http 服务器。
- [Memory sink](./builtin/memory.md)：输出到 eKuiper 内存主题以形成规则管道。
- [Log sink](./builtin/log.md)：写入日志，通常只用于调试。
- [Nop sink](./builtin/nop.md)：不输出，用于性能测试。

## 预定义的动作插件

我们已经开发了一些官方的动作插件。这些插件可以在 eKuiper 的源代码中找到，用户需要手动构建它们。详细信息请查看每个动作的构建和使用方法。

这些插件有预编译的二进制文件，用于主流cpu架构，如AMD或ARM。预编译的插件托管在 `https：//packages.emqx.net/kuiper-plugins/$version/$os/sinks/$type_$arch.zip` 中。例如，要获得用于 debian amd64 的 tdengine 插件，请从 `https：//packages.emqx.net/kuiper-plugins/1.4.4/debian/sinks/tdengine_amd64.zip` 安装。

预定义的动作插件列表。

- [Zero MQ sink](./plugin/zmq.md)：输出到 Zero MQ 。
- [File sink](./plugin/file.md)： 写入文件。
- [InfluxDB sink](./plugin/influx.md)： 写入 Influx DB 。
- [Tdengine sink](./plugin/tdengine.md)： 写入 Tdengine 。
- [Redis sink](./plugin/redis.md)： 写入 redis 。
- [Image sink](./plugin/image.md)： 写入一个图像文件。仅用于处理二进制结果。

### 缓存

动作用于将处理结果发送到外部系统中，存在外部系统不可用的情况，特别是在从边到云的场景中。例如，在弱网情况下，边到云的网络连接可能会不时断开和重连。因此，动作提供了缓存功能，用于在发送错误的情况下暂存数据，并在错误恢复之后自动重发缓存数据。动作的缓存可分为内存和磁盘的两级存储。用户可配置内存缓存条数，超过上限后，新的缓存将离线存储到磁盘中。缓存将同时保存在内存和磁盘中，这样缓存的容量就变得更大了；它还将持续检测故障恢复状态，并在不重新启动规则的情况下重新发送。

离线缓存的保存位置根据 `etc/kuiper.yaml` 里的 store 配置决定，默认为 sqlite 。如果磁盘存储是sqlite，所有的缓存将被保存到`data/cache.db`文件。每个 sink 将有一个唯一的 sqlite 表来保存缓存。缓存的计数添加到 sink 的 指标中的 buffer length 部分。

#### 流程

每个sink都可以配置自己的缓存机制。每个 sink 的缓存流程是相同的。如果启用了缓存，所有 sink 的事件都会经过两个阶段：首先是将所有内容保存到缓存中；然后在收到ack后删除缓存。

- 错误检测：发送失败后，sink应该通过返回特定的错误类型来识别可恢复的失败（网络等），这将返回一个失败的ack，这样缓存就可以被保留下来。对于成功的发送或不可恢复的错误，将发送一个成功的 ack 来删除缓存。
- 缓存机制：缓存将首先被保存在内存中。如果超过了内存的阈值，后面的缓存将被保存到磁盘中。一旦磁盘缓存超过磁盘存储阈值，缓存将开始rotate，即内存中最早的缓存将被丢弃，并加载磁盘中最早的缓存来代替。
- 重发策略：目前缓存机制仅可运行在默认的同步模式中，如果有一条消息正在发送中，则会等待发送的结果以继续发送下个缓存数据。否则，当有新的数据到来时，发送缓存中的第一个数据以检测网络状况。如果发送成功，将按顺序链式发送所有内存和磁盘中的所有缓存。链式发送可定义一个发送间隔，防止形成消息风暴。

#### 配置

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