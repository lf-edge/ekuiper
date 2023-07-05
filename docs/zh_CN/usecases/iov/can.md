# CAN 总线数据的流式处理

eKuiper 支持处理 CAN 总线数据。
它可以通过 socketCan 直接连接 CAN 总线数据，或通过其他协议中连接处理CAN总线数据，例如MQTT。

在本教程中，我们将指导你创建流和规则，并通过这两种方式处理 CAN 总线数据。

## 先决条件

DBC 文件定义了 CAN 总线的信号。我们使用 DBC 文件将 CAN 总线数据解码为可读信号。
因此，在运行这个演示之前，你需要准备DBC文件。我们已经在 `dbc` 文件夹中准备了样本 dbc 文件。你可以用你自己的dbc文件替换它们。

## 通过 SocketCAN 连接

SocketCAN 是 Linux 内核中的一个网络协议实现。它提供了一个基于套接字的接口，用于与 CAN 设备进行通信。

### 设置 CAN 接口

如果你用硬件连接到 CAN 总线，你就会有一个本地的 CAN 接口。
使用 `ip link show` 命令可以列出所有的接口，`link/can` 类型的接口就是 CAN 接口。

如果你没有一个真正的 CAN 接口，你可以使用虚拟的 CAN 接口。以 Ubuntu 为例，我们可以通过以下命令启用一个虚拟 CAN 接口：

```bash
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set up can0
```

通过 `ip link show` 命令检查接口，你会看到接口 `vcan0` 已经创建。

### 发送和接收 CAN 数据

我们将安装 `can-utils` 来发送/接收CAN数据。

```bash
sudo apt install can-utils
```

然后我们可以通过以下命令接收和打印原始CAN数据：

```bash
candump can0
```

在另一个终端，我们可以发送 CAN 数据进行测试：

```bash
cansend can0 123#1122334455667788
```

其中，`123` 是 CAN ID，`1122334455667788` 是数据有效载荷。
请确保数据在第一个终端被打印出来。到次为止，我们的CAN接口已经准备好了。

下一节，我们将使用 `cansend` 来发送测试数据到 eKuiper。

### 创建规则来处理 CAN 数据

首先，我们需要创建一个流来连接到虚拟can接口 `can0` 。流的定义如下：

```sql
create stream canDemo () WITH (TYPE="can", CONF_KEY="default", FORMAT="can", SHARED="true", SCHEMAID="dbc")
```

- `TYPE="can"`： 流的类型是`can'，它将通过 socketCan 连接到 CAN 总线。
- `CONF_KEY="default"`： 配置键是`default`，这将使用配置文件中的默认配置。默认配置在 `etc/sources/can.yaml`中，定义了can地址为 `can0` 。你可以在 `data/sources/can.yaml` 中用你自己的配置进行覆盖。
- `FORMAT="can"`： 数据的格式是 `can` ，它将把每个 CAN 帧的原始 CAN 数据解析成类似信号的键值对的内部数据，可以进行规则处理。
- `SHARED="true"`： 流是共享的，这意味着流将被所有规则共享。
- `SCHEMAID="dbc"`： 流的模式是 `dbc`，它将使用 `dbc` 文件夹内的 DBC 文件来解析原始 CAN 数据。

下一步，我们可以创建一个规则来打印数据：

```json
{
  "id": "canAll",
  "sql": "Select * From canDemo",
  "actions": [
    {
      "log": {}
    }
  ]
}
```

我们创建一个最简单的规则，名为 `canAll`，从流 `canDemo` 中选择所有数据并打印出来。
它将展示原始数据被解析成一个信号的键值对。

### 测试规则

通过 `cansend` 命令将测试数据发送到 CAN 接口 `can0` ：

```bash
cansend can0 586#5465737400000000
```

确保数据可以适配你的 DBC 文件。
其中，ID 必须在 DBC 文件中呈现，数据有效载荷必须与 DBC 文件中定义的长度相同。

然后检查规则 `canAll` 的日志，你应该收到这样的信息：

```json
{
  "VBBrkCntlAccelPedal": 0,
  "VBTOSLatPstn":   87.125,
  "VBTOSLonPstn":   168.75,
  "VBTOSObjID":     0,
  "VBTOSTTC":       46.400000000000006
}
```

## 通过其他协议以及 CANJSON 格式连接

由于安全或隐私的原因，我们可能不希望直接连接到CAN总线上。
通常情况下，用户会有一个网关来接收 CAN 数据，并通过其他协议（如 TCP. UDP 或 MQTT ）将其发送给应用程序。网关通常将把多个CAN帧打包成一个消息。

在本节中，我们将以 MQTT 为例来说明如何处理来自其他协议的 CA N数据。打包的格式可能是私有的。我们将使用 CANJSON 格式为例，将多个 CAN 帧打包成一个 JSON 来发送数据。

### 创建规则来处理 CAN 数据

首先，我们需要创建一个流来连接到 MQTT 来接收数据。流的定义如下：

```sql
create stream mqttCanDemo () WITH (TYPE="mqtt", CONF_KEY="default", FORMAT="canjson", SHARED="true", SCHEMAID="dbc", DATASOURCE="canDemo")
```

- `TYPE="mqtt"`： 流的类型是 `mqtt` ，它将连接到一个 MQTT 服务并订阅一个主题。
- `DATASOURCE="canDemo"`： 要订阅的主题是 `canDemo` 。你可以把它改成你自己的主题。
- `CONF_KEY="default"`： 配置键是 `default` ，它位于 `etc/mqtt_source.yaml` 文件中，定义了 MQTT 连接属性。
- `FORMAT="canjson"`： 数据的格式是 `canjson` ，它将把带有多个 can 帧信息的 JSON 解析成信号的键值对。
- `SHARED="true"`： 流是共享的，这意味着流将被所有规则共享。
- `SCHEMAID="dbc"`： 流的模式是 `dbc` ，它将使用 `dbc` 文件夹内的 DBC 文件来解析原始 CAN 数据。

然后我们可以创建一个规则来打印数据：

```json
{
  "id": "canAll2",
  "sql": "Select * From mqttCanDemo",
  "actions": [
    {
      "log": {}
    }
  ]
}
```

我们创建一个最简单的规则，名为 `canAll2` ，从流 `mqttCanDemo` 中选择所有数据并打印出来。
它将显示原始数据被解析成一个信号的键值对。

### 测试规则

发送测试数据到 MQTT 主题 `canDemo`。
这些帧可以包含任何数量的 CAN 帧。
确保每个 CAN 帧的 ID 都在 DBC 文件中定义。

```json
{
   "frames": [
      {
         "id": 1006,
         "data": "54657374000000005465737400000000"
      },
      {
         "id": 1414,
         "data": "5465737400000000"
      }
   ]
}
```

我们将得到类似的输出：

```json
{
  "VBBrkCntlAccelPedal": 0,
  "VBTOSLatPstn":   87.125,
  "VBTOSLonPstn":   168.75,
  "VBTOSObjID":     0,
  "VBTOSTTC":       46.400000000000006
}
```

## 进一步处理

现在，我们有了键值格式的解析后的 CAN 数据。我们可以对数据做进一步的处理，就像我们从 MQTT 或其他协议收到的 JSON 数据一样。查看文档以了解更多情况。
