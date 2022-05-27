# 可用的源

在 eKuiper 源代码中，有内置源和扩展源。

## 内置源

用户可以直接使用标准 eKuiper 实例中的内置源。内置源的列表如下。

- [Mqtt source](./builtin/mqtt.md)：从mqtt主题读取数据。
- [Neuron source](./builtin/neuron.md): 从本地 Neuron 实例读取数据。
- [EdgeX source](./builtin/edgex.md): 从 EdgeX foundry 读取数据。
- [Http pull source](./builtin/http_pull.md)：从http服务器中拉取数据。
- [Memory source](./builtin/memory.md)：从 eKuiper 内存主题读取数据以形成规则管道。
- [File source](./builtin/file.md)：从文件中读取数据，通常用作表格。

## 预定义的源插件

我们已经开发了一些官方的源码插件。这些插件可以在 eKuiper 的源代码中找到，用户需要手动构建它们。关于如何构建和使用，请查看每个源的文档。

这些插件有预编译的二进制文件，用于主流的cpu架构，如AMD或ARM。预编译建的插件托管在 `https://packages.emqx.net/kuiper-plugins/$version/$os/sources/$type_$arch.zip` 。例如，要获得 debian amd64 的 zmq 源插件，请从 `https://packages.emqx.net/kuiper-plugins/1.4.4/debian/sources/zmq_amd64.zip` 安装。

预定义的源插件列表：

- [Zero MQ source](./plugin/zmq.md)：从Zero MQ读取数据。
- [Random source](./plugin/random.md): 一个生成随机数据的源，用于测试。

## 源的使用

用户通过流或者表的方式来使用源。在创建的流属性中，需要把类型 `TYPE` 属性设置成所需要的源的名字。用户还可以在创建流的过程中，配置各种源通用的属性，例如解码类型（默认为 JSON）等来改变源的行为。创建流支持的通用属性和创建语法，请参考[流规格](../../sqls/streams.md)。