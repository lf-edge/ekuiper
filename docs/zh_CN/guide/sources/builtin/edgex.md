# EdgeX 数据源

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

eKuiper 内置支持 EdgeX 数据源，支持订阅来自于 [EdgeX 消息总线](https://github.com/edgexfoundry/go-mod-messaging)的数据，并将数据放入 eKuiper 数据处理流水线中。用户可直接通过 EdgeX 数据源消费 EdgeX 中的事件，[无需任何手动模式定义](#拓展阅读-edgex-中的流定义)。

在 eKuiper 中，EdgeX 连接器可以作为源连接器（从 EdgeX 获取数据）或 [Sink 连接器](../../sinks/builtin/mqtt.md)（将数据发布到 EdgeX），本节重点介绍 EdgeX 源连接器。

## Configurations

eKuiper 连接器可以通过[环境变量](../../../configuration/configuration.md#environment-variable-syntax)、[REST API](../../../api/restapi/configKey.md) 或配置文件进行配置，本节将介绍配置文件的使用方法。

EdgeX 源连接器的配置文件位于： `$ekuiper/etc/sources/edgex.yaml`，其中：

- default：对应全局连接配置。
- 自定义部分：适用于需要自定义连接参数的场景，该部分的配置将覆盖全局连接配置。
- 连接器重用：eKuiper 还支持通过 [`connectionSelector`](../../connector.md#connection-selector) 配置项在不同的配置中复用某个连接配置。

以下示例包括一个全局配置和自定义配置 `demo1`：

```yaml
#全局 Edgex 配置
default:
  protocol: tcp
  server: localhost
  port: 5573
  topic: rules-events
  messageType: event
#  optional:
#    ClientId: client1
#    Username: user1
#    Password: password

#覆盖全局配置
demo1: #Conf_key
  protocol: tcp
  server: 10.211.55.6
  port: 5571
  topic: rules-events
```

## 全局配置

用户可在 `default` 部分指定全局设置。

### 连接相关配置

- `protocol`：连接到 EdgeX 消息总线的协议，缺省为 `tcp`
- `server`：EdgeX 消息总线的地址，缺省为 `localhost`
- `port`：EdgeX 消息总线的端口，缺省为 `5573`

### 连接重用

- `connectionSelector`：重用 EdgeX 数据源连接，如下方配置示例中的 `edgex.redisMsgBus`。注意：连接配置文件位于 `connections/connection.yaml`。有关连接重用的详细解释，见[连接器的重用](../../connector.md#连接器的重用)。

  ```yaml
  #全局 Edgex 配置
  default:
  protocol: tcp
  server: localhost
  port: 5573
  connectionSelector: edgex.redisMsgBus
  topic: rules-events
  messageType: event
  #  optional:
  #    ClientId: client1
  #    Username: user1
  #    Password: password
  ```

  ::: tip

  指定 `connectionSelector` 参数后，所有关于连接的参数都会被忽略，包括 `protocol`、`server` 和 `port` 配置。本例中，`protocol: tcp | server: localhost | port: 5573`的值都将被忽略。

  :::

### 主题和消息配置

- `topic`：EdgeX 消息总线上监听的主题名称，缺省为 `rules-events`。用户可以直接连接到 EdgeX 消息总线上的主题也可以连接到 application service 暴露的主题。需要注意的是，两种主题的消息数据类型不同，需要设置正确的 messageType 类型。

- `type`：EdgeX 消息总线类型，目前支持三种消息总线。如果指定的消息总线类型不支持，将使用缺省 `zero` 类型。
  - `zero`：使用 ZeroMQ 类型的消息总线。
  - `mqtt`：使用 MQTT 服务器作为消息总线，如选择 MQTT 总线类型，eKuiper 支持更多的 MQTT 配置项，具体请查看 [其他配置（MQTT 相关配置）](#其他配置-mqtt-相关配置)。
  - `redis`：使用 Redis 服务器作为消息总线。使用 EdgeX docker compose 启动时，type 参数会默认设置为该类型。

  EdgeX Levski 引入了两种信息消息总线类型，eKuiper 从 1.7.1 开始支持这两种新的类型，分别为
  - `nats-jetstream`
  - `nats-core`

- `messageType`：EdgeX 消息模型类型。该参数支持两种类型：
  - `event`：如果连接到 EdgeX application service 的主题、则消息为 "event" 类型；消息将会解码为 `dtos.Event` 类型。该选项为默认值。
  - `request`：如果直接连接到消息总线的主题，接收 device service 或者 core data 发出的数据，则消息类型为 "request"。消息将会解码为 `requests.AddEventRequest` 类型。

### 其他配置（MQTT 相关配置）

如使用 MQTT 消息总线，eKuiper 还支持其他一些可选配置项。请注意，所有可选配置都应为**字符类型**，`KeepAlive: "5000"` ，有关各配置项的详细解释，可参考 MQTT 协议。

- `ClientId`

- `Username`
- `Password`
- `Qos`
- `KeepAlive`
- `Retained`
- `ConnectionPayload`
- `CertFile`
- `KeyFile`
- `CertPEMBlock`
- `KeyPEMBlock`
- `SkipCertVerify`

## 自定义配置

对于需要自定义某些连接参数的场景，eKuiper 支持用户创建自定义模块来实现全局配置的重载。

**配置示例**

```yaml
#覆盖全局配置
demo1: #Conf_key
  protocol: tcp
  server: 10.211.55.6
  port: 5571
  topic: rules-events
```

定义 `demo1` 配置组后，如希望在创建流时使用此配置，可通过 `CONF_KEY` 选项并指定配置名称，此时，在自定义配置中定义的参数将覆盖 `default` 配置中的相应参数。详细步骤，可参考 [流语句](../../../sqls/streams.md)。

**示例**

```sql
create stream demo1() WITH (FORMAT="JSON"、type="edgex"、CONF_KEY="demo1");
```

## 创建流类型源

完成连接器的配置后，后续可通过创建流将其与 eKuiper 规则集成。EdgeX 源连接器可以作为[流类型](../../streams/overview.md)或[扫描表类型数据源](../../tables/scan.md)使用，本节将以流类型源为例进行说明。

### 通过 REST API 创建

REST API 为 eKuiper 提供了一种可编程的交互方式，适用于自动化或需要将 eKuiper 集成到其他系统中的场景。

**示例**

```sql
create stream demo1() WITH (FORMAT="JSON"、type="edgex"、CONF_KEY="demo1");
```

详细操作步骤及命令解释，可参考 [通过 REST API 进行流管理](../../../api/restapi/streams.md)。

### 通过 CLI 创建

用户也可以通过命令行界面（CLI）直接访问 eKuiper。

1. 进入 eKuiper `bin` 目录：

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. 使用 `create` 命令创建规则，指定 EdgeX 连接器为数据源，例如：

   ```bash
   bin/kuiper CREATE STREAM demo'() with(format="json"、datasource="demo" type="edgex")'
   ```

详细操作步骤及命令解释，可参考 [通过 CLI 进行流管理](../../../api/cli/streams.md)。

### 拓展阅读：EdgeX 中的流定义

EdgeX 在 [reading objects](https://docs.edgexfoundry.org/2.0/microservices/core/data/Ch-CoreData/#events-and-readings) 已经定义了数据类型，因此在 eKuiper 中建议采用 [schema-less 方式](../../streams/overview.md#schema)的 EdgeX 流式定义，如下所示：

```shell
# cd $eKuiper_base
# bin/kuiper CREATE STREAM demo'() with(format="json"、datasource="demo" type="edgex")'
```

### 自动数据类型转换

eKuiper 在处理 EdgeX 事件时，会根据 EdgeX `ValueType` 字段自动管理数据类型转换。

**数据转换原则：**

- 如果在 reading 的值类型中可以找到支持的数据类型，执行数据类型转换；guas
- 如果在 reading 的值类型中找不到支持的数据类型，将保留原值；
- 如果类型转换失败，该值将被**丢弃**，并在日志上打印一条告警消息；

#### Boolean

如果 `reading` 中 `ValueType` 的值为 `Bool` ，那么 eKuiper 会试着将其转换为 `boolean` 类型：

- 转换为 `true`： "1"、"t"、"T"、"true"、"TRUE"、"True"
- 转换为 `false`："0"、"f"、"F"、"false"、"FALSE"、"False"

#### Bigint

如果 `reading` 中 `ValueType` 的值为 `INT8`、`INT16`、`INT32`、`INT64`、`UINT8`、`UINT16`、`UINT32`、`UINT64` 那么 eKuiper 会试着将其转换为 `Bigint` 类型。

#### Float

如果 `reading` 中 `ValueType` 的值为 `FLOAT32`、`FLOAT64` ，那么 eKuiper 会试着将其转换为 `Float` 类型。

#### String

如果 `reading` 中 `ValueType` 的值为 `String`，那么 eKuiper 会试着将其转换为 `String` 类型。

#### Boolean 数组

EdgeX 中的 `Bool` 数组类型会被转换为 `boolean` 数组。

#### Bigint 数组

EdgeX 中所有的 `INT8`，`INT16`，`INT32`，`INT64`，`UINT8`，`UINT16`，`UINT32`，`UINT64` 数组类型会被转换为 `Bigint` 数组。

#### Float 数组

EdgeX 中所有的 `FLOAT32`，`FLOAT64` 数组类型会被转换为 `Float` 数组。
