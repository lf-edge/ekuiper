# EdgeX 规则引擎教程

## 概览

在 EdgeX Geneva 版本中, [EMQ X Kuiper - 基于 SQL 的轻量级流式数据处理软件](https://github.com/emqx/kuiper)与 EdgeX 进行了集成。在进入这篇教程之前，让我们先花一些时间来了解一些 Kuiper 的基本知识。EMQ X Kuiper 是 Golang 实现的轻量级物联网边缘分析、流式处理开源软件，可以运行在各类资源受限的边缘设备上。Kuiper 基于`源 (Source)`，`SQL (业务逻辑处理)`， `目标 (Sink)` 的方式来支持流式数据处理。

- 源（Source）：流式数据的数据源，例如来自于 MQTT 服务器的数据。在 EdgeX 的场景下，数据源就是 EdgeX 消息总线（EdgeX message bus），可以是来自于 ZeroMQ 或者 MQTT 服务器；
- SQL：SQL 是你流式数据处理指定业务逻辑的地方，Kuiper 提供了 SQL 语句可以对数据进行抽取、过滤和转换；
- 目标（Sink）：目标用于将分析结果发送到特定的目标。例如，将分析结果发送到另外的 MQTT 服务器，或者一个 HTTP Rest 地址；

![](../../resources/arch.png)

使用 Kuiper，一般需要完成以下三个步骤。

- 创建流，就是你定义数据源的地方
- 写规则
  - 为数据分析写 SQL
  - 指定一个保存分析结果的目标
- 部署，并且运行规则

该教程描述如何使用 Kuiper 处理来自于 EdgeX 消息总线的数据。

## Kuiper EdgeX 集成

在不同的微服务之间，EdgeX 使用[消息总线](https://github.com/edgexfoundry/go-mod-messaging)进行数据交换。它包含了一个抽象的消息总线接口，并且实现了 ZeroMQ 与 MQTT (注：Kuiper 目前**只支持 ZeroMQ** 消息总线 ，MQTT 将在后续版本中加入）在不同的微服务之间信息交互的支持。Kuiper 和 EdgeX 的集成工作包含了以下三部分，

- 扩展了一个 EdgeX 消息总线源，支持从 EdgeX 消息总线中接收数据  

- 为了可以分析数据，Kuiper 需知道传入的数据流的格式。一般来说，用户最好在创建流的时候指定被分析的流数据的格式。如下所示，一个 ``demo`` 流包含了一个名为 ``temperature`` 的字段。这与在关系型数据库中创建表格定义的时候非常像。在创建了流定义以后，Kuiper 可以在编译或者运行时对进入的数据进行类型检查，相应错误也会报告给用户。

  ```shell
  CREATE STREAM demo (temperature bigint) WITH (FORMAT="JSON"...)
  ```

  然而在 EdgeX 中，数据类型定义在 EdgeX ``Core contract Service`` 中已经指定，为了提升使用体验，用户可以在创建流的时候不指定数据类型。Kuiper 源会在初始化规则的时候，从 ``Core contract Service`` 中获取所有的 ``value descriptors`` 定义（所以如果有任何数据类型定义的变化，你需要重启规则）。当接收到来自于消息总线的数据的时候，会根规则转换为[相应的数据类型](../rules/sources/edgex.md)。

- 扩展支持 EdgeX 消息总线目标（sink），用于将处理结果写回至 EdgeX 消息总线。用户也可以选择将分析结果发送到 Kuiper 之前已经支持的 RestAPI 接口等。

![](arch_light.png)

## 运行 EdgeX Docker 实例

在 EdgeX Geneva 版本正式发布后，你可以按照[这个文档](https://fuji-docs.edgexfoundry.org/Ch-QuickStart.html)来启动所有的服务。但是因为目前 EdgeX Geneva 还未正式发布，所以如果现在想试用 Kuiper 的话，不得不手工从[这里](https://github.com/edgexfoundry/developer-scripts/blob/master/releases/nightly-build/compose-files/docker-compose-nexus-mongo-no-secty.yml)下载 Docker Composer 文件，然后启动所有的 EdgeX 容器。

```shell
# wget https://github.com/edgexfoundry/developer-scripts/raw/master/releases/nightly-build/compose-files/docker-compose-nexus-mongo-no-secty.yml

# docker-compose -f ./docker-compose-nexus-redis-no-secty.yml up -d --build
```

所哟的容器启动完毕之后，请使用 ``docker ps`` 命令确定所有的容器已经正常启动。

```
TODO: The docker instance list
```

### 创建流

该步骤是创建一个可以从 EdgeX 消息总线进行数据消费的流。有两种方法来支持管理流，你可以选择喜欢的方式。

#### 方式1: 使用 Rest API

请将 ``$your_server`` 替换为本地运行的 Kuiper 实例的地址。

```shell
curl -X POST \
  http://$TODO:9081/streams \
  -H 'Content-Type: application/json' \
  -d '{
  "sql": "create stream demo() WITH (FORMAT=\"JSON\", TYPE=\"edgex\")"
}'
```

关于其它 API，请参考[该文档](../restapi/overview.md).

#### 方式2: 使用 Kuiper 命令行

使用以下命令，进入运行中的 Kuiper docker 实例。

```shell
docker exec -it kuiper /bin/sh
```

使用以下命令，创建一个名为 ``demo`` 的流定义.

```shell
bin/cli create stream demo'() WITH (FORMAT="JSON", TYPE="edgex")'
```

其它命令行，请参考[该文档](../cli/overview.md)。

------

现在流已经创建好了，但是你可能好奇 Kuiper 是如何知道消息总线的地址和端口，因为此类信息在 ``CREATE STREAM`` 并未指定。实际上这些信息是在配置文件  ``etc/sources/edgex.yaml`` 中指定的，你可以在命令行窗口中输入 ``cat etc/sources/edgex.yaml`` 来查看文件的内容。如果你有不同的服务器、端口和服务的地址，请更新相应的配置。正如之前提到的，这些配置选项可以在容器启动的时候进行重写。

```yaml
#Global Edgex configurations
default:
  protocol: tcp
  server: localhost
  port: 5563
  topic: events
  serviceServer: http://localhost:48080
.....  
```

更多关于配置文件的信息，请参考[该文档](../rules/sources/edgex.md).

### 创建规则

让我们创建一条规则，将分析结果发送至 MQTT 服务器，关于 MQTT 目标的相关配置，请参考[这个链接](../rules/sinks/mqtt.md)。与创建流的过程类似，你可以选择使用 REST 或者命令行来管理规则。

以下例子将过滤所有 ``randomnumber`` 小于 31 的数据，分析结果将被发布到公共的 MQTT 服务器 ``broker.emqx.io`` 的主题``result`` 上。 

#### 选项1: 使用 Rest API

```shell
curl -X POST \
  http://$TODO:9081/rules \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "rule1",
  "sql": "SELECT * FROM demo WHERE randomnumber > 30",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "result",
        "clientId": "demo_001"
      }
    }
  ]
}'
```

#### 选项2: 使用 Kuiper 命令行

你可以使用任意编辑器来创建一条规则，将下列内容拷贝到编辑器中，并命名为 ``rule.txt``。

```
{
  "sql": "SELECT * from demo where randomnumber > 30",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "result",
        "clientId": "demo_001"
      }
    }
  ]
}
```

在运行的容器中，执行以下命令。

```shell
# bin/cli create rule rule1 -f rule.txt
Connecting to 127.0.0.1:20498...
Creating a new rule from file rule.txt.
Rule rule1 was created, please use 'cli getstatus rule $rule_name' command to get rule status.
```

------

如想将结果发送到别的目标，请参考 Kuiper 中支持的[其它目标](../rules/overview.md#actions)。你现在可以看一下在 ``log/stream.log``中的日志文件，查看规则的详细信息。

```
//TODO
```

### 监控分析结果

因为所有的分析结果都被发布到``tcp://broker.emqx.io:1883``，你可以直接使用以下的  ``mosquitto_sub`` 命令来监听结果，你也可以参考别的 [MQTT 客户端工具](https://www.emqx.io/blog/mqtt-client-tools).

```shell
# mosquitto_sub -h broker.emqx.io -t result
//TODO
...
```

你发现，只有那些 randomnumber 大于 30 被发布到了 ``result`` 主题。

你也可以敲入以下的命令来查看规则执行的状态。相关的查看规则状态的 REST API 也有提供，请检查[相关文档](../restapi/overview.md).

```shell
# bin/cli getstatus rule rule1
Connecting to 127.0.0.1:20498...
{
  "source_demo_0_records_in_total": 29,
  "source_demo_0_records_out_total": 29,
  "source_demo_0_exceptions_total": 0,
  "source_demo_0_process_latency_ms": 0,
  "source_demo_0_buffer_length": 0,
  "source_demo_0_last_invocation": "2020-03-19T10:30:09.294337",
  "op_preprocessor_demo_0_records_in_total": 29,
  "op_preprocessor_demo_0_records_out_total": 29,
  "op_preprocessor_demo_0_exceptions_total": 0,
  "op_preprocessor_demo_0_process_latency_ms": 0,
  "op_preprocessor_demo_0_buffer_length": 0,
  "op_preprocessor_demo_0_last_invocation": "2020-03-19T10:30:09.294355",
  "op_filter_0_records_in_total": 29,
  "op_filter_0_records_out_total": 21,
  "op_filter_0_exceptions_total": 0,
  "op_filter_0_process_latency_ms": 0,
  "op_filter_0_buffer_length": 0,
  "op_filter_0_last_invocation": "2020-03-19T10:30:09.294362",
  "op_project_0_records_in_total": 21,
  "op_project_0_records_out_total": 21,
  "op_project_0_exceptions_total": 0,
  "op_project_0_process_latency_ms": 0,
  "op_project_0_buffer_length": 0,
  "op_project_0_last_invocation": "2020-03-19T10:30:09.294382",
  "sink_sink_mqtt_0_records_in_total": 21,
  "sink_sink_mqtt_0_records_out_total": 21,
  "sink_sink_mqtt_0_exceptions_total": 0,
  "sink_sink_mqtt_0_process_latency_ms": 0,
  "sink_sink_mqtt_0_buffer_length": 1,
  "sink_sink_mqtt_0_last_invocation": "2020-03-19T10:30:09.294423"
}
```

### 总结

在本教程中，我们介绍了使用 EdgeX Kuiper 规则引擎的非常简单的例子，如果使用过程中发现任何问题，请到 EdgeX，或者 Kuiper Github 中报问题。

### 更多练习

目前的规则没有过滤发送给 Kuiper 的任何数据，那么如何过滤数据呢？例如，如果你只关心 ``Int32`` 字段中大于30的数据，请使用[删除规则](../cli/rules.md)，然后将规则更新如下。

```
{
  "sql": "SELECT * from demo WHERE Int32 > 30",
  "actions": [...]
}
```

完成更改后，重新部署规则。这时候如果监听 MQTT 服务的结果主题，你可以发现只有 ``Int32`` 字段中大于30的数据被发送至该主题。

#### 扩展阅读

- 阅读 [EdgeX 源](../rules/sources/edgex.md) 获取更多详细信息，以及类型转换等。
- [如何使用 meta 函数抽取在 EdgeX 消息总线中发送的更多信息？](edgex_meta.md) 设备服务往总线上发送数据的时候，一些额外的信息也随之发送，比如时间创建时间，id 等。如果你想在 SQL 语句中使用这些信息，请参考这篇文章。
- [EdgeX 消息总线目标](../rules/sinks/edgex.md). 该文档描述了如何使用 EdgeX 消息总线目标。如果你想把分析结果发送到消息总线中，你可能对此文章感兴趣。 

如想了解更多的 EMQ X Kuiper 的信息，请参考以下资源。

- [Kuiper Github 代码库](https://github.com/emqx/kuiper/)
- [Kuiper 参考指南](https://github.com/emqx/kuiper/blob/master/docs/zh_CN/reference.md)

