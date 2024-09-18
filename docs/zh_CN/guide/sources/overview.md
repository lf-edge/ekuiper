# 数据源

在 eKuiper 源代码中，有内置源和扩展源。

## 读入模式

源连接器提供了与外部系统的连接，以便将数据加载进来。关于数据加载机制，有两种模式。

- 扫描：像一个由事件驱动的流一样，一个一个地加载数据事件。这种模式的源可以用在流或扫描表中。
- 查找：在需要时引用外部内容，只用于查找表。

每个源将支持一种或两种模式。在源页面上，如果支持该模式，会有一个徽章显示。

## 内置源

用户可以直接使用标准 eKuiper 实例中的内置源。内置源的列表如下。

- [Mqtt source](./builtin/mqtt.md)：从mqtt 主题读取数据。
- [Neuron source](./builtin/neuron.md): 从本地 Neuron 实例读取数据。
- [EdgeX source](./builtin/edgex.md): 从 EdgeX foundry 读取数据。
- [Http pull source](./builtin/http_pull.md)：从 http 服务器中拉取数据。
- [Http push source](./builtin/http_push.md)：通过 http 推送数据到 eKuiper。
- [Redis source](./builtin/redis.md): 从 Redis 中查询数据，用作查询表。
- [RedisSub source](./builtin/redisSub.md): 从 Redis 频道中订阅数据。
- [File source](./builtin/file.md)：从文件中读取数据，通常用作表格。
- [Memory source](./builtin/memory.md)：从 eKuiper 内存主题读取数据以形成规则管道。
- [Simulator source](./builtin/simulator.md)：生成模拟数据，用于测试。

## 预定义的源插件

我们已经开发了一些官方的源码插件。这些插件可以在 eKuiper 的源代码中找到，用户需要手动构建它们。关于如何构建和使用，请查看每个源的文档。

这些插件有预编译的二进制文件，用于主流的cpu架构，如AMD或ARM。预编译建的插件托管在 `https://packages.emqx.net/kuiper-plugins/$version/$os/sources/$type_$arch.zip` 。例如，要获得 debian amd64 的 zmq 源插件，请从 `https://packages.emqx.net/kuiper-plugins/1.4.4/debian/sources/zmq_amd64.zip` 安装。

预定义的源插件列表：

- [SQL source](./plugin/sql.md): 定期从关系数据库中拉取数据。
- [Random source](./plugin/random.md): 一个生成随机数据的源，用于测试。
- [Zero MQ source](./plugin/zmq.md)：从 Zero MQ 读取数据。
- [Kafka source](./plugin/kafka.md)： 从 Kafka 中读取数据

## 源的使用

用户通过流或者表的方式来使用源。在创建的流属性中，需要把类型 `TYPE` 属性设置成所需要的源的名字。用户还可以在创建流的过程中，配置各种源通用的属性，例如解码类型（默认为 JSON）等来改变源的行为。创建流支持的通用属性和创建语法，请参考[流规格](../../sqls/streams.md)。

## 运行时节点

用户在创建规则时，数据源是一个逻辑节点。根据数据源本身的类型和用户配置的不同，运行时每个数据源有可能会生成由多个节点组成的执行计划。数据源属性配置项众多，实际运行时的逻辑颇为复杂。通过拆分执行计划为多个节点，主要有以下好处：

- 各种数据源之间有大量共有的属性及其实现逻辑，例如数据格式的解码。将共有属性实现拆分为独立的运行时节点，有利于节点的重用，简化数据源节点的实现（单一职责原则），提高节点的可维护行。
- 数据源的属性中包含耗时的计算，例如解压缩，解码等。通过单一节点的指标，难以区分数据源实际执行时子任务的运行状态。拆分节点之后，可以支持更细粒度的运行时指标，了解每个子任务的状态和时延。
- 子任务拆分后，可以实现并行的计算，提高规则整体的运行效率。

### 执行计划拆分

数据源节点的物理执行计划可拆分为：

Connector --> RateLimit --> Decompress --> Decode --> Preprocess

每个节点生成的条件为：

- Connector: 每个数据源必定实现，用于连接外部数据源并读取数据到系统中。
- RateLimit: 数据源类型为推送源（如 MQTT，以订阅/推送而非拉取的方式读入数据的源），且配置了 `interval`
  属性。该节点用于在数据源头控制数据流入的频率。详情请参考[降采样](./down_sample.md)
- Decompress: 数据源类型读取字节码数据（如 MQTT，允许发送任何字节码而非固定格式），且配置了 `decompress` 属性。该节点用于解压缩数据。
- Decode: 如数据源类型读取字节码数据，且配置了 `format` 属性。该节点将根据格式配置以及格式相关的 schema 配置，实现字节码的反序列化。
- Preprocess: 流定义中显式定义了 schema 且 `strictValidation` 打开。该节点将根据 schema
  定义验证并转换原始数据。请注意，若输入数据需要频繁做类型转换，该节点可能会有大量额外的性能损耗。
