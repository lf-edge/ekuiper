# 数据连接

在流式数据处理中，与数据源和目标端点的无缝集成能力非常关键。作为一款轻量级的边缘流处理引擎，eKuiper通过**连接器**实现了与外部系统的数据交互，如数据库、消息中间件等。

eKuiper 内置各类连接器并支持用户以插件的形式扩展连接器，因此能从各种来源获取数据，对其进行实时处理，并将处理结果推送至指定系统（数据 Sink），因此能够轻松融入从 IoT 边缘设备到云计算基础设施的各种场景。

eKuiper 连接器主要分两类：

- **数据源连接器**：负责从各类外部数据源中导入数据至 eKuiper。
- **数据 Sink 连接器**：负责将 eKuiper 处理后的数据输出至外部系统。

本章将介绍 eKuiper 中各类连接器的配置、使用以及最佳实践。

## 数据源连接器

eKuiper [数据源连接器](./sources/overview.md)旨在从各种外部源导入数据到平台。在 eKuiper 中，用户只需将这些数据源集成到他们的数据流或表中，即可将相关数据导入 eKuiper 并执行查询或数据转换等操作。eKuiper 还提供了丰富的配置选项，方便满足用户的各类数据处理需求。

**内置源连接器：**

eKuiper 内置以下数据源连接器：

- [MQTT 源](./sources/builtin/mqtt.md)：从 MQTT 主题读取数据。
- [Neuron 源](./sources/builtin/neuron.md)：从本地 Neuron 实例读取数据。
- [EdgeX 源](./sources/builtin/edgex.md)：从 EdgeX foundry 读取数据。
- [HTTP Pull 源](./sources/builtin/http_pull.md)：从 HTTP 服务器中拉取数据。
- [HTTP Push 源](./sources/builtin/http_push.md)：通过 HTTP 推送数据到 eKuiper。
- [文件源](./sources/builtin/file.md)：从文件中读取数据，通常用作表格。
- [内存源](./sources/builtin/memory.md)：从 eKuiper 内存主题读取数据，常用于构建[规则管道](./rules/rule_pipeline.md)。

- [Redis 源](./sources/builtin/redis.md)：从 Redis 中查询数据，用作查询表。

**插件式源连接器**
对于需要自定义数据源或与特定第三方集成的场景，eKuiper 提供了基于插件的拓展源连接器：

- [SQL 源](./sources/plugin/sql.md)：定期从关系数据库中拉取数据。
- [视频源](./sources/plugin/video.md)：用于查询视频流。
- [Random 源](./sources/plugin/random.md)：用于生成随机数据的源，用于测试。
- [Zero MQ 源](./sources/plugin/zmq.md)：从 Zero MQ 读取数据。
- [Kafka 源](./sources/plugin/kafka.md): 从 Kafka 读取数据

## 数据 Sink 连接器

eKuiper Sink 连接器负责将 eKuiper 处理后的数据发送到各种目标端点或系统，可直接与 MQTT、Neuron、EdgeX 等平台对接，并提供缓存机制以应对网络中断场景，确保数据的一致性。此外，用户还可通过动态属性和资源重用来定制接收行为，简化集成并提高可伸缩性。

与源连接器类似，Sink 连接器也分为内置和插件式两种。

**内置 Sink 连接器**

以下是 eKuiper 提供的内置 Sink 连接器：

- [MQTT Sink](./sinks/builtin/mqtt.md)：输出到外部 MQTT 服务。
- [Neuron Sink](./sinks/builtin/neuron.md)：输出到本地的 Neuron 实例。
- [EdgeX Sink](./sinks/builtin/edgex.md)：输出到 EdgeX Foundry。此动作仅在启用 edgex 编译标签时存在。
- [Rest Sink](./sinks/builtin/rest.md)：输出到外部 HTTP 服务器。
- [Redis Sink](./sinks/builtin/redis.md)：写入 Redis 。
- [File Sink](./sinks/builtin/file.md)：写入文件。
- [Memory Sink](./sinks/builtin/memory.md)：输出到 eKuiper 内存主题,，常用于构建[规则管道](./rules/rule_pipeline.md)。
- [Log Sink](./sinks/builtin/log.md)：写入日志，通常只用于调试。
- [Nop Sink](./sinks/builtin/nop.md)：不输出，用于性能测试。

**插件式 Sink 连接器**

对于特殊的数据分发或特定平台集成需求，eKuiper 支持基于插件的 Sink 连接器：

- [InfluxDB Sink](./sinks/plugin/influx.md)：输出到 Influx DB `v1.x`。
- [InfluxDBV2 Sink](./sinks/plugin/influx2.md)：输出到 Influx DB `v2.x`。
- [Image Sink](./sinks/plugin/image.md)：输出到一个图像文件。仅用于处理二进制结果。
- [Zero MQ Sink](./sinks/plugin/zmq.md)：输出到 ZeroMQ。
- [Kafka Sink](./sinks/plugin/kafka.md)：输出到 Kafka。

### 数据模板

eKuiper [数据模板](./sinks/data_template.md) 支持用户对分析结果进行"二次处理"，以满足不同接收系统的多样化格式要求。利用 Golang 模板系统，eKuiper 提供了动态数据转换、条件输出和迭代处理的机制，确保了与各种接收器的兼容性和精确格式化。

## 批量配置

eKuiper 提供了 Memory、File、MQTT 等多种数据连接器。为进一步简化用户的配置流程，eKuiper 通过 REST API 引入了批量配置功能，支持用户同时导入或导出多个配置。

示例

```json
{
    "streams": { ... },
    "tables": { ... },
    "rules": { ... },
    "nativePlugins": { ... },
    "portablePlugins": { ... },
    "sourceConfig": { ... },
    "sinkConfig": { ... },
    ...
}
```

具体操作步骤，可参考 [数据导入导出管理](../api/restapi/data.md)。
