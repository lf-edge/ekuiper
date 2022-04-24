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