# Neuron 数据源

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

eKuiper 的 Neuron 连接器可订阅本地 Neuron 实例的消息。需要注意的是，该源仅可用于本地的 Neuron，因为与 Neuron 的通信基于 nanomsg IPC 协议，无法通过网络进行。

在 eKuiper 中，Neuron 数据源可以作为源连接器（从 Neuron 代理获取数据）或 [Sink 连接器](../../sinks/builtin/mqtt.md)（将数据发布到 Neuron），本节重点介绍 Neuron 源连接器。此外，在 eKuiper 端，所有 Neuron 源和 Sink 共享同一个 Neuron 连接。

::: tip 异步拨号机制

注意：由于到 Neuron 数据源的连接拨号采用异步拨号模式，因此系统会在后台持续尝试，直到成功建立连接。因此，即便 Neuron 服务暂停，使用 Neuron Sink 的规则也不会显示错误。在调试过程中，您可以检查规则状态，验证消息流的接收数量是否正常。

:::

## 配置

eKuiper 连接器可以通过[环境变量](../../../configuration/configuration.md#environment-variable-syntax)、[REST API](../../../api/restapi/configKey.md) 或配置文件进行配置，本节将介绍配置文件的使用方法。

Neuron 源连接器的配置文件位于：`$ekuiper/etc/sources/neuron.yaml`。

```yaml
default:
  # The nng connection url to connect to the neuron
  url: tcp://127.0.0.1:7081
ipc:
  url: ipc:///tmp/neuron-ekuiper.ipc
```

以上示例提供了两种连接方式，默认 TCP 连接到本地服务的 7081 端口，以及用于本地进程间通信的 IPC 机制。

::: tip

指定的端口应与 Neuron 实例的端口相对应。在此示例中，我们使用了 Neuron 默认的端口 7081，请根据实际情况调整。

:::

你可以通过 api 的方式提前检查对应 sink 端点的连通性: [连通性检查](../../../api/restapi/connection.md#连通性检查)

## Neuron 事件格式

Neuron 事件通常采用以下 JSON 格式：

```json
{
  "timestamp": 1646125996000,
  "node_name": "node1",
  "group_name": "group1",
  "values": {
    "tag_name1": 11.22,
    "tag_name2": "string"
  },
  "errors": {
    "tag_name3": 122
  }
}
```

## 创建流数据源

完成连接器的配置后，后续可通过创建流将其与 eKuiper 规则集成。Neuron 源连接器可以作为[流式](../../streams/overview.md)或[扫描表数据源](../../tables/scan.md)使用，本节将以流类型源为例进行说明。

您可通过 REST API 或 CLI 工具在 eKuiper 中创建 Neuron 数据源。

### 通过 REST API 创建

REST API 为 eKuiper 提供了一种可编程的交互方式，适用于自动化或需要将 eKuiper 集成到其他系统中的场景。

**示例**

```sql
CREATE STREAM neuron_stream () WITH (FORMAT="json", TYPE="neuron");
```

详细操作步骤及命令解释，可参考 [通过 REST API 进行流管理](../../../api/restapi/streams.md)。

### 通过 CLI 创建

用户也可以通过命令行界面（CLI）直接访问 eKuiper。

1. 进入 eKuiper `bin` 目录：

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. 使用 `create` 命令创建规则，指定 Neuron 数据源，如：

   ```bash
   ./kuiper create stream neuron_stream ' WITH (FORMAT="json", TYPE="neuron")'
   ```

详细操作步骤及命令解释，可参考 [通过 CLI 进行流管理](../../../api/cli/streams.md)。
