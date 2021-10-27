# EdgeX 规则引擎教程

## 概览

在 EdgeX Geneva 版本中, [LF Edge eKuiper - 基于 SQL 的轻量级流式数据处理软件](https://github.com/lf-edge/ekuiper)与 EdgeX 进行了集成。在进入这篇教程之前，让我们先花一些时间来了解一些 eKuiper 的基本知识。 eKuiper 是 Golang 实现的轻量级物联网边缘分析、流式处理开源软件，可以运行在各类资源受限的边缘设备上。eKuiper 基于`源 (Source)`，`SQL (业务逻辑处理)`， `目标 (Sink)` 的方式来支持流式数据处理。

- 源（Source）：流式数据的数据源，例如来自于 MQTT 服务器的数据。在 EdgeX 的场景下，数据源就是 EdgeX 消息总线（EdgeX message bus），可以是来自于 ZeroMQ 或者 MQTT 服务器；
- SQL：SQL 是你流式数据处理指定业务逻辑的地方，eKuiper 提供了 SQL 语句可以对数据进行抽取、过滤和转换；
- 目标（Sink）：目标用于将分析结果发送到特定的目标。例如，将分析结果发送到另外的 MQTT 服务器，或者一个 HTTP Rest 地址；

![](../arch.png)

使用 eKuiper，一般需要完成以下三个步骤。

- 创建流，就是你定义数据源的地方
- 写规则
  - 为数据分析写 SQL
  - 指定一个保存分析结果的目标
- 部署，并且运行规则

该教程描述如何使用 eKuiper 处理来自于 EdgeX 消息总线的数据。

## eKuiper EdgeX 集成

在不同的微服务之间，EdgeX 使用[消息总线](https://github.com/edgexfoundry/go-mod-messaging)进行数据交换。它包含了一个抽象的消息总线接口，并分别实现了 ZeroMQ 与 MQTT，在不同的微服务之间信息交互的支持。eKuiper 和 EdgeX 的集成工作包含了以下三部分，

- 扩展了一个 EdgeX 消息总线源，支持从 EdgeX 消息总线中接收数据  

- 为了可以分析数据，eKuiper 需知道传入的数据流的格式。一般来说，用户最好在创建流的时候指定被分析的流数据的格式。如下所示，一个 ``demo`` 流包含了一个名为 ``temperature`` 的字段。这与在关系型数据库中创建表格定义的时候非常像。在创建了流定义以后，eKuiper 可以在编译或者运行时对进入的数据进行类型检查，相应错误也会报告给用户。

  ```shell
  CREATE STREAM demo (temperature bigint) WITH (FORMAT="JSON"...)
  ```

  然而在 EdgeX 中，数据类型定义在 EdgeX event/reading 中已经指定，为了提升使用体验，用户可以在创建流的时候不指定数据类型。当接收到来自于消息总线的数据的时候，会根规则转换为[相应的数据类型](../rules/sources/edgex.md)。

- 扩展支持 EdgeX 消息总线目标（sink），用于将处理结果写回至 EdgeX 消息总线。用户也可以选择将分析结果发送到 eKuiper 之前已经支持的 RestAPI 接口等。

![](./arch_light.png)

## 迁移到 EdgeX V2

eKuiper v1.2.1 之后的版本将仅支持 EdgeX v2 ( Ireland 及之后的版本 )，并引入以下突破性变化。

1. EdgeX 源不再依赖 `Core contract Service` 。用户可以从配置文件 `edgex.yaml` 中移除属性 `serviceServer` 的相关配置。
2. [元数据中的突破性变化](./edgex_meta.md#突破性变化)。例如，元数据 `Device` 重命名为 `DeviceName` 。

## 运行 EdgeX Docker 实例

打开 [EdgeX compose 项目](https://github.com/edgexfoundry/edgex-compose)，并且下载 Ireland 版本的 Docker compose file，然后启动所有的 EdgeX 容器。

```shell
# docker-compose -f ./docker-compose-no-secty.yml up -d --build
```

所有的容器启动完毕之后，请使用 ``docker ps`` 命令确定所有的容器已经正常启动。

```shell
$ docker ps
CONTAINER ID   IMAGE                                                           COMMAND                  CREATED
 STATUS          PORTS                                                                                  NAMES
c7cb2c07dc4f   nexus3.edgexfoundry.org:10004/device-virtual:latest             "/device-virtual --c…"   13 minutes ago   Up 13 minutes   127.0.0.1:59900->59900/tcp                                                             edgex-device-virtual
d7089087c301   nexus3.edgexfoundry.org:10004/device-rest:latest                "/device-rest --cp=c…"   13 minutes ago   Up 13 minutes   127.0.0.1:59986->59986/tcp                                                             edgex-device-rest
32cd339157e2   nexus3.edgexfoundry.org:10004/app-service-configurable:latest   "/app-service-config…"   13 minutes ago   Up 13 minutes   48095/tcp, 127.0.0.1:59701->59701/tcp                                                  edgex-app-rules-engine
62c2174d4b45   nexus3.edgexfoundry.org:10004/sys-mgmt-agent:latest             "/sys-mgmt-agent -cp…"   13 minutes ago   Up 13 minutes   127.0.0.1:58890->58890/tcp                                                             edgex-sys-mgmt-agent
5b9f9cfb4307   nexus3.edgexfoundry.org:10004/core-data:latest                  "/core-data -cp=cons…"   13 minutes ago   Up 13 minutes   127.0.0.1:5563->5563/tcp, 127.0.0.1:59880->59880/tcp                                   edgex-core-data
b455b06e2e7c   nexus3.edgexfoundry.org:10004/core-command:latest               "/core-command -cp=c…"   13 minutes ago   Up 13 minutes   127.0.0.1:59882->59882/tcp                                                             edgex-core-command
6de994ce09d6   nexus3.edgexfoundry.org:10004/core-metadata:latest              "/core-metadata -cp=…"   13 minutes ago   Up 13 minutes   127.0.0.1:59881->59881/tcp                                                             edgex-core-metadata
1b62bf57dd34   nexus3.edgexfoundry.org:10004/support-notifications:latest      "/support-notificati…"   13 minutes ago   Up 13 minutes   127.0.0.1:59860->59860/tcp                                                             edgex-support-notifications
38776815a286   nexus3.edgexfoundry.org:10004/support-scheduler:latest          "/support-scheduler …"   13 minutes ago   Up 13 minutes   127.0.0.1:59861->59861/tcp                                                             edgex-support-scheduler
5176ddff9f08   emqx/kuiper:1.2.1-alpine                                        "/usr/bin/docker-ent…"   13 minutes ago   Up 13 minutes   9081/tcp, 20498/tcp, 127.0.0.1:59720->59720/tcp                                        edgex-kuiper
c78419bc5096   consul:1.9.5                                                    "docker-entrypoint.s…"   13 minutes ago   Up 13 minutes   8300-8302/tcp, 8301-8302/udp, 8600/tcp, 8600/udp, 127.0.0.1:8500->8500/tcp             edgex-core-consul
d4b236a7b561   redis:6.2.4-alpine                                              "docker-entrypoint.s…"   13 minutes ago   Up 13 minutes   127.0.0.1:6379->6379/tcp                                                               edgex-redis
```

### 原生 (native) 方式运行

出于运行效率考虑，读者可能需要直接以原生方式运行 eKuiper，但是可能会发现直接使用下载的 eKuiper
软件包启动后[无法直接使用 EdgeX](https://github.com/lf-edge/ekuiper/issues/596)，这是因为 EdgeX 缺省消息总线依赖于 `zeromq` 库，如果 eKuiper
启动的时候在库文件寻找路径下无法找到 `zeromq` 库，它将无法启动。这导致对于不需要使用 EdgeX 的 eKuiper 用户也不得不去安装 `zeromq` 库 ，因此缺省提供的下载安装包中**<u>内置不支持
Edgex</u>** 。如果读者需要以原生方式运行 eKuiper 并且支持 `EdgeX`，可以通过命令 `make pkg_with_edgex` 自己来编译原生安装包，或者从容器中直接拷贝出安装包。

## 创建流

该步骤是创建一个可以从 EdgeX 消息总线进行数据消费的流。有两种方法来支持管理流，你可以选择喜欢的方式。

### 方式1: 使用 Rest API

请注意: EdgeX 中的 eKuiper Rest 接口使用``59720``端口，而不是缺省的``9081``端口。所以在 EdgeX 调用 eKuiper Rest 的时候，请将文档中所有的 9081 替换为 59720。

请将 ``$eKuiper_server`` 替换为本地运行的 eKuiper 实例的地址。

```shell
curl -X POST \
  http://$eKuiper_server:59720/streams \
  -H 'Content-Type: application/json' \
  -d '{
  "sql": "create stream demo() WITH (FORMAT=\"JSON\", TYPE=\"edgex\")"
}'
```

关于其它 API，请参考[该文档](../restapi/overview.md).

### 方式2: 使用 eKuiper 命令行

使用以下命令，进入运行中的 eKuiper docker 实例。

```shell
docker exec -it kuiper /bin/sh
```

使用以下命令，创建一个名为 ``demo`` 的流定义.

```shell
bin/kuiper create stream demo'() WITH (FORMAT="JSON", TYPE="edgex")'
```

其它命令行，请参考[该文档](../cli/overview.md)。

------

现在流已经创建好了，但是你可能好奇 eKuiper 是如何知道消息总线的地址和端口，因为此类信息在 ``CREATE STREAM`` 并未指定。实际上这些信息是在配置文件  ``etc/sources/edgex.yaml`` 中指定的，你可以在命令行窗口中输入 ``cat etc/sources/edgex.yaml`` 来查看文件的内容。如果你有不同的服务器、端口和服务的地址，请更新相应的配置。正如之前提到的，这些配置选项可以在容器启动的时候进行重写。

```yaml
#Global Edgex configurations
default:
  protocol: tcp
  server: localhost
  port: 5566
  topic: events
.....  
```

更多关于配置文件的信息，请参考[该文档](../rules/sources/edgex.md).

## 创建规则

让我们创建一条规则，将分析结果发送至 MQTT 服务器，关于 MQTT 目标的相关配置，请参考[这个链接](../rules/sinks/mqtt.md)。与创建流的过程类似，你可以选择使用 REST 或者命令行来管理规则。

以下例子将选出所有 ``events`` 主题上所有的数据，分析结果将被

- 发布到公共的 MQTT 服务器 ``broker.emqx.io`` 的主题``result`` 上；
- 打印至日志文件

### 选项1: 使用 Rest API

```shell
curl -X POST \
  http://$eKuiper_server:9081/rules \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "result",
        "clientId": "demo_001"
      }
    },
    {
      "log":{}
    }
  ]
}
```

### 选项2: 使用 eKuiper 命令行

你可以使用任意编辑器来创建一条规则，将下列内容拷贝到编辑器中，并命名为 ``rule.txt``。

```json
{
  "sql": "SELECT * from demo",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "result",
        "clientId": "demo_001"
      }
    },
    {
      "log":{}
    }
  ]
}
```

在运行的容器中，执行以下命令。

```shell
# bin/kuiper create rule rule1 -f rule.txt
Connecting to 127.0.0.1:20498...
Creating a new rule from file rule.txt.
Rule rule1 was created successfully, please use 'cli getstatus rule rule1' command to get rule status.
```

------

如想将结果发送到别的目标，请参考 eKuiper 中支持的[其它目标](../rules/overview.md#目标动作)。你现在可以看一下在 ``log/stream.log``中的日志文件，查看规则的详细信息。

```
time="2021-07-08 01:03:08" level=info msg="Serving kuiper (version - 1.2.1) on port 20498, and restful api on http://0.0.0.0:59720. \n" file="server/server.go:144"
Serving kuiper (version - 1.2.1) on port 20498, and restful api on http://0.0.0.0:59720. 
time="2021-07-08 01:08:14" level=info msg="Successfully subscribed to edgex messagebus topic rules-events." file="extensions/edgex_source.go:111" rule=rule1
time="2021-07-08 01:08:14" level=info msg="The connection to server tcp://broker.emqx.io:1883 was established successfully" file="sinks/mqtt_sink.go:182" rule=rule1
time="2021-07-08 01:08:20" level=info msg="sink result for rule rule1: [{\"Float32\":-2.4369560555943686e+38}]" file="sinks/log_sink.go:16" rule=rule1
time="2021-07-08 01:08:20" level=info msg="sink result for rule rule1: [{\"Float64\":-1.488582e+308}]" file="sinks/log_sink.go:16" rule=rule1
time="2021-07-08 01:08:20" level=info msg="sink result for rule rule1: [{\"Uint64\":9544048735510870974}]" file="sinks/log_sink.go:16" rule=rule1
time="2021-07-08 01:08:20" level=info msg="sink result for rule rule1: [{\"Uint16\":33714}]" file="sinks/log_sink.go:16" rule=rule1
time="2021-07-08 01:08:20" level=info msg="sink result for rule rule1: [{\"Uint8\":57}]" file="sinks/log_sink.go:16" rule=rule1
time="2021-07-08 01:08:20" level=info msg="sink result for rule rule1: [{\"Uint32\":3860684797}]" file="sinks/log_sink.go:16" rule=rule1
...
```

## 监控分析结果

因为所有的分析结果都被发布到``tcp://broker.emqx.io:1883``，你可以直接使用以下的  ``mosquitto_sub`` 命令来监听结果，你也可以参考别的 [MQTT 客户端工具](https://www.emqx.cn/blog/mqtt-client-tools).

```shell
# mosquitto_sub -h broker.emqx.io -t result
[{"Bool":false}]
[{"Int64":228212448717749920}]
[{"Int8":-70}]
[{"Int16":16748}]
[{"Int32":728167766}]
[{"Uint16":32311}]
[{"Uint8":133}]
[{"Uint64":16707883778643919729}]
[{"Uint32":1453300043}]
[{"Bool":false}]
[{"Float32":1.3364580409833176e+37}]
[{"Float64":8.638344e+306}]
[{"Int64":-2517790659681968229}]
[{"Int16":-31683}]
[{"Int8":96}]
[{"Int32":-1245869667}]
...
```

你也可以敲入以下的命令来查看规则执行的状态。相关的查看规则状态的 REST API 也有提供，请检查[相关文档](../restapi/overview.md).

```shell
# bin/kuiper getstatus rule rule1
Connecting to 127.0.0.1:20498...
{
  "source_demo_0_records_in_total": 29,
  "source_demo_0_records_out_total": 29,
  "source_demo_0_exceptions_total": 0,
  "source_demo_0_process_latency_ms": 0,
  "source_demo_0_buffer_length": 0,
  "source_demo_0_last_invocation": "2020-04-17T10:30:09.294337",
  "op_filter_0_records_in_total": 29,
  "op_filter_0_records_out_total": 21,
  "op_filter_0_exceptions_total": 0,
  "op_filter_0_process_latency_ms": 0,
  "op_filter_0_buffer_length": 0,
  "op_filter_0_last_invocation": "2020-04-17T10:30:09.294362",
  "op_project_0_records_in_total": 21,
  "op_project_0_records_out_total": 21,
  "op_project_0_exceptions_total": 0,
  "op_project_0_process_latency_ms": 0,
  "op_project_0_buffer_length": 0,
  "op_project_0_last_invocation": "2020-04-17T10:30:09.294382",
  "sink_mqtt_0_0_records_in_total": 21,
  "sink_mqtt_0_0_records_out_total": 21,
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_process_latency_ms": 0,
  "sink_mqtt_0_0_buffer_length": 1,
  "sink_mqtt_0_0_last_invocation": "2020-04-17T10:30:09.294423"
```

## 总结

在本教程中，我们介绍了使用 EdgeX eKuiper 规则引擎的非常简单的例子，如果使用过程中发现任何问题，请到 EdgeX，或者 eKuiper Github 中报问题。

## 更多练习

目前的规则没有过滤发送给 eKuiper 的任何数据，那么如何过滤数据呢？请使用[删除规则](../cli/rules.md)，然后试着更改一下 SQL 语句，完成更改后，重新部署规则。这时候如果监听 MQTT 服务的结果主题，检查一下相关的规则是否起作用？

### 扩展阅读

- 从 eKuiper 0.9.1 版本开始，通过一个单独的 Docker 镜像提供了 [可视化 web 用户交互界面](../manager-ui/overview.md)，您可以通过该 web 界面进行流、规则和插件等管理。
- 阅读 [EdgeX 源](../rules/sources/edgex.md) 获取更多详细信息，以及类型转换等。
- [如何使用 meta 函数抽取在 EdgeX 消息总线中发送的更多信息？](edgex_meta.md) 设备服务往总线上发送数据的时候，一些额外的信息也随之发送，比如时间创建时间，id 等。如果你想在 SQL 语句中使用这些信息，请参考这篇文章。
- [eKuiper 中使用 Golang 模版 (template) 定制分析结果](../rules/data_template.md) 分析结果在发送给不同的 sink 之前，可以使用数据模版对结果进行二次处理，参考这片文章可以获取更多的关于数据模版的使用场景。
- [EdgeX 消息总线目标](../rules/sinks/edgex.md). 该文档描述了如何使用 EdgeX 消息总线目标。如果想把你的分析结果被别的 EdgeX 服务消费，你可以通过这个 sink 发送 EdgeX 格式的数据，别的 EdgeX 服务可以通过这个 eKuiper sink 暴露出来的新的消息总线进行订阅。
- [eKuiper 插件开发教程](../plugins/plugins_tutorial.md): eKuiper 插件机制基于 Go 语言的插件机制，使用户可以构建松散耦合的插件程序，在运行时动态加载和绑定，如果您对开发插件有兴趣，请参考该文章。

如想了解更多的 LF Edge eKuiper 的信息，请参考以下资源。

- [eKuiper Github 代码库](https://github.com/lf-edge/ekuiper/)
- [eKuiper 参考指南](https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/reference.md)

