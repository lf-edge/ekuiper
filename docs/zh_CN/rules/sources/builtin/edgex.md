

# EdgeX 源

eKuiper 提供了内置的 EdgeX 源支持，它可以被用来订阅来自于[EdgeX 消息总线](https://github.com/edgexfoundry/go-mod-messaging)的数据，并且将数据放入 eKuiper 数据处理流水线中。

## EdgeX 流定义

EdgeX 在 [readings](https://docs.edgexfoundry.org/2.0/microservices/core/data/Ch-CoreData/#events-and-readings) 已经定义了数据类型，因此在 eKuiper 中建议采用 schema-less 方式的 EdgeX 流式定义，如下所示。

```shell
# cd $eKuiper_base
# bin/kuiper CREATE STREAM demo'() with(format="json", datasource="demo" type="edgex")'
```

EdgeX 源会试图取得某个字段的类型，

- 如果在 reading 的值类型中可找到其数据类型，就将其转换为对应类型；
- 如果在 reading 的值类型中可找不到到其数据类型，将保留原值；
- 如果类型转换失败，该值将被**丢弃**，并在日志上打印一条告警消息；

在 readings 中定义的数据类型，将被转换为 eKuiper 中相应支持的[数据类型](../../../sqls/streams.md)。

### Boolean

如果 `reading` 中  `ValueType` 的值为 `Bool` ，那么 eKuiper 会试着将其转换为 `boolean` 类型，以下的值将被转化为 `true`。

- "1", "t", "T", "true", "TRUE", "True" 

以下值将被转换为 `false`。

- "0", "f", "F", "false", "FALSE", "False"

### Bigint

如果 `reading` 中  `ValueType` 的值为 `INT8` , `INT16`, `INT32`,  `INT64` , `UINT8` , `UINT16` ,  `UINT32` , `UINT64` 那么 eKuiper 会试着将其转换为 `Bigint` 类型。 

### Float

如果 `reading` 中  `ValueType` 的值为 `FLOAT32`, `FLOAT64` ，那么 eKuiper 会试着将其转换为 `Float` 类型。 

### String

如果 `reading` 中  `ValueType` 的值为 `String`，那么 eKuiper 会试着将其转换为 `String` 类型。

### Boolean 数组

EdgeX 中的 `Bool` 数组类型会被转换为 `boolean` 数组。

### Bigint 数组

EdgeX 中所有的 `INT8` , `INT16`, `INT32`,  `INT64` , `UINT8` , `UINT16` ,  `UINT32` , `UINT64` 数组类型会被转换为 `Bigint` 数组。

### Float 数组

EdgeX 中所有的 `FLOAT32`, `FLOAT64`  数组类型会被转换为 `Float` 数组。 

# 全局配置

EdgeX 源配置文件为 `$ekuiper/etc/sources/edgex.yaml`，以下配置文件内容。

```yaml
#Global Edgex configurations
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
```

用户可以在此指定全局的 EdgeX 配置。在 `default` 部分中指定的配置将作为所有 EdgeX 源的缺省配置。

## protocol

连接到 EdgeX 消息总线的协议，缺省为 `tcp`

## server

EdgeX 消息总线的地址，缺省为 `localhost`

## port

EdgeX 消息总线的端口，缺省为 `5573`

## connectionSelector

重用 EdgeX 源连接。连接配置信息位于 `connections/connection.yaml`.
```yaml
edgex:
  redisMsgBus: #connection key
    protocol: redis
    server: 127.0.0.1
    port: 6379
    type: redis
    #  Below is optional configurations settings for mqtt
    #  type: mqtt
    #  optional:
    #    ClientId: client1
    #    Username: user1
    #    Password: password
    #    Qos: 1
    #    KeepAlive: 5000
    #    Retained: true/false
    #    ConnectionPayload:
    #    CertFile:
    #    KeyFile:
    #    CertPEMBlock:
    #    KeyPEMBlock:
    #    SkipCertVerify: true/false
```
对于 EdgeX 连接，这里有一个配置组。用户应该使用 `edgex.redisMsgBus` 来作为参数。举例如下：
```yaml
#Global Edgex configurations
default:
  protocol: tcp
  server: localhost
  port: 5573
  connectionSelector: edgex.redisMsgBus
  topic: rules-events
  messageType: event
```
*注意*: 相应配置组一旦指定 connectionSelector 参数，所有关于连接的参数都会被忽略. 上面例子中，` protocol: tcp | server: localhost | port: 5573` 会被忽略。


## topic

EdgeX 消息总线上监听的主题名称，缺省为 `rules-events`。用户可以直接连接到 EdgeX 消息总线上的主题也可以连接到 application service 暴露的主题。需要注意的是，两种主题的消息数据类型不同，需要设置正确的
messageType 类型。

## type

EdgeX 消息总线类型，目前支持三种消息总线。如果指定了错误的消息总线类型，那么会使用缺省 `zero` 类型。

- `zero`：使用 ZeroMQ 类型的消息总线
- `mqtt`：使用 MQTT 服务器作为消息总线
- `redis`: 使用 Redis 服务器作为消息总线。使用 EdgeX docker compose 启动时，type参数会默认设置为该类型。

### messageType

EdgeX 消息模型类型。如果连接到 EdgeX application service 的 topic, 则消息为 "event" 类型。否则，如果直接连接到消息总线的 topic，接收到 device service 或者 core
data 发出的数据，则消息类型为 "request"。该参数支持两种类型：

- `event`: 消息将会解码为 `dtos.Event` 类型。该选项为默认值。
- `request`: 消息将会解码为 `requests.AddEventRequest` 类型。

## optional

如果使用了 MQTT 消息总线，还可以指定别的一些可选配置项。请注意，所有在可选的配置项里指定的值都必须为**<u>字符类型</u>**，因此这里出现的所有的配置应该是字符类型的 - 例如 `KeepAlive: "5000"`
。以下为支持的可选的配置列表，您可以参考 MQTT 协议规范来获取更详尽的信息。

- ClientId

- Username
- Password
- Qos
- KeepAlive
- Retained
- ConnectionPayload
- CertFile
- KeyFile
- CertPEMBlock
- KeyPEMBlock
- SkipCertVerify

## 重载缺省设置

在某些情况下，你可能想消费来自于多个主题的数据。eKuiper 支持指定别的配置，并且在创建流定义的时候使用 `CONF_KEY` 来指定新的配置。

```yaml
#覆盖全局配置
demo1: #Conf_key
  protocol: tcp
  server: 10.211.55.6
  port: 5571
  topic: rules-events
```

如果你有个特定的源需要覆盖缺省的设置，你可以定义一个自定义的配置段。在上面的例子中，我们创建了一个新的配置 `demo1`，然后你在创建流定义的时候可以使用选项 `CONF_KEY` 来使用新的配置 (参考 [流定义规范](../../../sqls/streams.md) 获取更多详细信息)。

**例子**

```
create stream demo1() WITH (FORMAT="JSON", type="edgex", CONF_KEY="demo1");
```

在自定义的配置中，能够使用的配置项与 `default` 部分的是一样的，任何在自定义段中设置的值将覆盖 `default` 部分里的配置。

