

# EdgeX 源

Kuiper 提供了内置的 EdgeX 源支持，它可以被用来订阅来自于[EdgeX 消息总线](https://github.com/edgexfoundry/go-mod-messaging)的数据，并且将数据放入 Kuiper 数据处理流水线中。

## EdgeX 流定义

EdgeX 在 [value descriptors](https://github.com/edgexfoundry/go-mod-core-contracts) 已经定义了数据类型，因此在 Kuiper 中建议采用 schema-less 方式的 EdgeX 流式定义，如下所示。

```shell
# cd $kuiper_base
# bin/cli CREATE STREAM demo'() with(format="json", datasource="demo" type="edgex")'
```

EdgeX 源会试图取得某个字段的类型，

- 如果在 value descriptors 中可找到其数据类型，就将其转换为对应类型；
- 如果在 value descriptors 中可找不到到其数据类型，将保留原值；
- 如果类型转换失败，该值将被**丢弃**，并在日志上打印一条告警消息；

在 EdgeX value descriptors 中定义的数据类型，将被转换为 Kuiper 中相应支持的[数据类型](../../sqls/streams.md)。

### Boolean

如果 ``ValueDescriptor`` 中  ``Type`` 的值为 ``Bool`` ，那么 Kuiper 会试着将其转换为 ``boolean`` 类型，以下的值将被转化为 ``true``。

- "1", "t", "T", "true", "TRUE", "True" 

以下值将被转换为 ``false``。

- "0", "f", "F", "false", "FALSE", "False"

### Bigint

如果 ``ValueDescriptor`` 中  ``Type`` 的值为 ``INT8`` , ``INT16``, ``INT32``,  ``INT64`` , ``UINT8`` , ``UINT16`` ,  ``UINT32`` , ``UINT64`` 那么 Kuiper 会试着将其转换为 ``Bigint`` 类型。 

### Float

如果 ``ValueDescriptor`` 中  ``Type`` 的值为 ``FLOAT32``, ``FLOAT64`` ，那么 Kuiper 会试着将其转换为 ``Float`` 类型。 

### String

如果 ``ValueDescriptor`` 中  ``Type`` 的值为 ``String``，那么 Kuiper 会试着将其转换为 ``String`` 类型。 

# 全局配置

EdgeX 源配置文件为 ``$kuiper/etc/sources/edgex.yaml``，以下配置文件内容。

```yaml
#Global Edgex configurations
default:
  protocol: tcp
  server: localhost
  port: 5573
  topic: events
  serviceServer: http://localhost:48080
#  optional:
#    ClientId: client1
#    Username: user1
#    Password: password
```

用户可以在此指定全局的 EdgeX 配置。在 ``default`` 部分中指定的配置将作为所有 EdgeX 源的缺省配置。

## protocol

连接到 EdgeX 消息总线的协议，缺省为 ``tcp``

## server

EdgeX 消息总线的地址，缺省为 ``localhost``

## port

EdgeX 消息总线的端口，缺省为 ``5573``.

## topic

EdgeX 消息总线上监听的主题名称，缺省为 ``events``.

## serviceServer

访问 value descriptors 的基础服务地址，配置项 ``serviceServer`` 的值与 ``/api/v1/valuedescriptor`` 拼接后，用于获取 EdgeX 服务器上定义的所有 value descriptors。

## 重载缺省设置

在某些情况下，你可能想消费来自于多个主题的数据。Kuiper 支持指定别的配置，并且在创建流定义的时候使用 ``CONF_KEY`` 来指定新的配置。

```yaml
#Override the global configurations
demo1: #Conf_key
  protocol: tcp
  server: 10.211.55.6
  port: 5571
  topic: events
```

如果你有个特定的源需要覆盖缺省的设置，你可以定义一个自定义的配置段。在上面的例子中，我们创建了一个新的配置 ``demo1``，然后你在创建流定义的时候可以使用选项 ``CONF_KEY`` 来使用新的配置 (参考 [流定义规范](../../sqls/streams.md) 获取更多详细信息)。

**例子**

```
create stream demo1() WITH (FORMAT="JSON", type="edgex", CONF_KEY="demo1");
```

在自定义的配置中，能够使用的配置项与 ``default`` 部分的是一样的，任何在自定义段中设置的值将覆盖 ``default`` 部分里的配置。

