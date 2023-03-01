# 使用 eKuiper 对 Neuron 采集的数据进行流式处理

[Neuron](https://neugates.io/zh) 是运行在各类物联网边缘网关硬件上的工业协议网关软件，旨在解决工业 4.0 背景下设备数据统一接入难的问题。通过将来自繁杂多样工业设备的不同协议类型数据转换为统一标准的物联网 MQTT 消息，实现设备与工业物联网系统之间、设备彼此之间的互联互通，进行远程的直接控制和信息获取，为智能生产制造提供数据支撑。

Neuron 支持同时为多个不同通讯协议设备、数十种工业协议进行一站式接入及 MQTT 协议转换，仅占用超低资源，即可以原生或容器的方式部署在 X86、ARM 等架构的各类边缘硬件中。同时，用户可以通过基于 Web 的管理控制台实现在线的网关配置管理。

在 eKuiper 1.5.0 之前的版本中，Neuron 与 eKuiper 之间需要采用 MQTT 作为中转。二者协同时，需要额外部署 MQTT broker。同时，用户需要自行处理数据格式，包括读入和输出时的解码编码工作。eKuiper 1.5.0 版本加入了 Neuron source 和 sink，使得用户无需配置即可在 eKuiper 中接入 Neuron 中采集到的数据进行计算；也可以方便地从 eKuiper 中通过 Neuron 控制设备 。两个产品的整合，可以显著降低边缘计算解决方案对资源的使用要求，降低使用门槛。

## Neuron 与 eKuiper 的集成

Neuron 2.0 中，北向应用增加了 eKuiper 支持。当 Neuron 开启北向 eKuiper 应用之后，二者之间通过 NNG 协议进行连接，从而显著降低网络通信消耗，提高性能。

![ekuiper to neuron](./ekuiper_to_neuron.png)

eKuiper 与 Neuron 之间的集成是双向的，其实现主要包含两个部分:

- 提供了一个 Neuron 源，支持从 Neuron 订阅数据。
- 提供了一个 Neuron sink, 支持通过 Neuron 控制设备。

典型的工业物联网边缘数据处理场景中，Neuron 和 eKuiper 部署在同一台边缘机器上。这也是目前二者集成所支持的场景。若需要通过网络进行通信，则仍然可以通过之前 MQTT 的方式进行协同。

Neuron 与 eKuiper 的连接经历了几个阶段：
1. 早期版本，双方采用 MQTT 作为中转。
2. Neuron 2.0 和 eKuiper 1.5 之后的版本，双方采用 IPC 协议一对一连接。
3. Neuron 2.4 和 eKuiper 1.9 之后的版本，双方采用 TCP 协议连接，可支持多对多连接。

## 准备工作

本教程将以工业物联网数据采集和清洗的场景为例，手把手地介绍如何一步一步完成云边协同的数据采集，数据清理和数据反控。

其中，Neuron 和 eKuiper 部署在靠近设备的边缘端网关，或者工控机上。Neuron 采集的数据经过 eKuiper 处理后发送到云端的 MQTT broker 以便于云端的应用进行下一步的处理。同时，eKuiper 可以接收云端 MQTT 的指令，通过 Neuron 控制本地的设备。

开始动手操作之前，需要准备以下环境：

- 云端的 MQTT broker，例如[快速开始 EMQX](https://www.emqx.io/docs/zh/v4.4/getting-started/getting-started.html#%E5%BF%AB%E9%80%9F%E5%BC%80%E5%A7%8B)。假设云端 MQTT broker 地址为 `tcp://cloud.host:1883`, 以下教程将以此地址为例。
- 为了方便观察运行结果，我们需要安装一个 MQTT 客户端，例如 [MQTT X](https://mqttx.app/) 。

## 快速部署

Neuron 和 eKuiper 都支持二进制安装包以及 Docker 容器化部署方案。本文以 Docker 方案为例，采用 [docker compose](https://docs.docker.com/compose/) 方式，一键完成边缘端两个组件的快速部署。

1. 复制 [docker-compose.yml](./docker-compose.yml) 文件到部署的机器上。其内容如下，包含了 Neuron，eKuiper 以及 eKuiper 的管理界面 eKuiper manager（可选）。其中，eKuiper 和 neuron 共享了名为 nng-ipc 的 volume ，用于二者通信。如果要使用 eKuiper alpine 版本，需要在 compose 文件的 eKuiper 部分添加 `user: root:root` 赋予写入 ipc 文件的权限，否则连接将无法建立。

   ```yaml
   version: '3.4'

   services:
      manager:
         image: emqx/ekuiper-manager:1.9
         container_name: ekuiper-manager
         ports:
            - "9082:9082"
      ekuiper:
         image: lfedge/ekuiper:1.9
         ports:
            - "9081:9081"
            - "127.0.0.1:20498:20498"
         container_name: ekuiper
         hostname: ekuiper
         environment:
            MQTT_SOURCE__DEFAULT__SERVER: "tcp://mybroker:1883"
            KUIPER__BASIC__CONSOLELOG: "true"
            KUIPER__BASIC__IGNORECASE: "false"
            # The default neuron url. Change it if you want to use another port.
            SOURCES__NEURON__DEFAULT__URL: "tcp://neuron:7081"
         volumes:
            - /tmp/data:/kuiper/data
            - /tmp/log:/kuiper/log
            # Enable the following line if you want to use the IPC mode to connect to earlier version of neuron
            # - nng-ipc:/tmp
      neuron:
         image: neugates/neuron:2.4.0
         ports:
            - "7001:7001"
            # The default port to communicate with eKuiper. Change it if you want to use another port.
            - "7081:7081"
         container_name: neuron
         hostname: neuron
         volumes:
            - /tmp/neuron/data:/opt/neuron/persistence
            # Enable the following line if you want to use the IPC mode to connect to earlier version of eKuiper
            # - nng-ipc:/tmp

      # Enable the following lines if you want to use the IPC mode to connect to earlier version of eKuiper and neuron
      # volumes:
      #  nng-ipc:
   ```
   用户可自定义配置连接端口，本例中为 7081。修改端口时，需要修改 Neuron 的 eKuiper 北向应用端口，同时修改本文件中用到该端口的部分，即 neuron 的端口暴露和 eKuiper 的环境变量默认连接 url 部分。

   > 各版本使用注意事项
   > 1. eKuiper 1.9 之后版本与 Neuron 2.4 之前版本对接只能通过 ipc，需要配置 `SOURCES__NEURON__DEFAULT__URL: "ipc:///tmp/neuron-ekuiper.ipc"`，并且启用 volumes nng-ipc 的配置。Neuron 无需暴露 7081 端口。
   > 2. eKuiper 1.9 之前版本与 neuron 2.4 之前版本对接只能通过 ipc，需要去除 `SOURCES__NEURON__DEFAULT__URL` 环境变量配置并且启用 volumes nng-ipc 的配置。Neuron 无需暴露 7081 端口。
   > 3. eKuiper 1.9 之前版本与 neuron 2.4 之后版本无法直接对接，可通过 MQTT 中转。
2. 在该文件所在目录，运行:
   
   ```shell
   # docker compose up -d
   ```

3. 所有的容器启动完毕之后，请使用 `docker ps` 命令确定所有的容器已经正常启动。

   ```shell
   CONTAINER ID   IMAGE                        COMMAND                  CREATED        STATUS          PORTS                                                NAMES
   3d61c1b166e5   neugates/neuron:2.0.1        "/usr/bin/entrypoint…"   18 hours ago   Up 11 seconds   127.0.0.1:7000-7001->7000-7001/tcp                   manager-neuron
   62a74d0be2ea   lfedge/ekuiper:1.5.0-slim    "/usr/bin/docker-ent…"   18 hours ago   Up 11 seconds   0.0.0.0:9081->9081/tcp, 127.0.0.1:20498->20498/tcp   manager-ekuiper
   7deffb470c1a   emqx/ekuiper-manager:1.5.0   "/usr/bin/docker-ent…"   18 hours ago   Up 11 seconds   0.0.0.0:9082->9082/tcp                               ekuiper-manager
   ```

## 配置 Neuron 和 eKuiper

Neuron 启动之后，我们需要配置 Neuron 的南向设备和北向 eKuiper 应用通道，然后启动模拟器进行模拟数据采集。

南向设备和模拟器配置，请参考[Neuron 快速教程](https://neugates.io/docs/zh/latest/getting-started/quick_start.html#%E8%B5%84%E6%BA%90%E5%87%86%E5%A4%87) ，完成到运行和使用的 3. 南向配置部分。该教程中的北向配置部分为 mqtt 应用，本教程需要采用 eKuiper 作为北向应用。

### Neuron 北向 eKuiper 应用配置

在配置菜单中选择`北向应用管理`，进入到北向应用管理界面，此时未添加任何应用，需要手动添加应用，在本例中，我们将创建一个 eKuiper 应用。

第一步，添加北向应用：

1. 点击右上角的`添加配置`按键；
2. 填写应用名称，例如，ekuiper-1；
3. 下拉框中显示在该软件版本中，我们可用的北向应用，此次我们选择 ekuiper 的插件，如下图所示。
![select app](https://neugates.io/docs/docs-assets/img/north-add.81cdfc27.png)
4. 创建应用成功之后，会在北向应用管理界面出现一个刚刚创建的应用的卡片，此时应用的工作状态在初始化，连接状态在断开连接状态中，如下图所示。
![neuron ekuiper app](./neuron_ekuiper_app.png)

第二步，订阅 Group：

点击第一步应用卡片 ekuiper-1 中任意空白处，进入到订阅Group界面，如下图所示。

![group](https://neugates.io/docs/docs-assets/img/subscriptions-add.7d591876.png)

1. 点击右上角的`添加订阅`按键添加订阅；
2. 下拉框选择南向设备，这里我们选择上面建好的 modbus-plus-tcp-1 的设备；
3. 下拉框选择所要订阅的 Group，这里我们选择上面建好的 group-1；
4. 点击提交，完成订阅。
5. 点击`北向应用管理`，点开应用卡片中的工作状态开关，使应用进入运行中的状态。

至此，Neuron 已配置好数据采集，并将采集到的数据发送到北向的 eKuiper 通道中。

### eKuiper manager 配置

eKuiper manager 是一个 Web 管理界面，可管理多个 eKuiper 实例。因此，我们需要设置 manager 管理的 eKuiper 实例。详细设置请参考[eKuiper 管理控制台的使用](../../operation/manager-ui/overview.md#开始使用)。

eKuiper 管理可使用 REST API，命令行以及管理控制台。以下教程中，我们主要使用 REST API 进行管理，包括流和规则的创建。

## 创建流

使用如下命令创建名为 `neuronStream` 的流。其中，`type` 属性设置为`neuron`，表示该流会连接到 neuron 中。neuron 中采集到的数据会全部发送过来，从而在 eKuiper 中多条规则都会针对同一份数据进行处理，因此流属性`shared`设置为 true。

```shell
curl -X POST --location http://127.0.0.1:9081/streams \
    -H 'Content-Type: application/json' \
    -d '{"sql":"CREATE STREAM neuronStream() WITH (TYPE=\"neuron\",FORMAT=\"json\",SHARED=\"true\");"}'
```

## 采集规则

Neuron 流建立之后，我们可以在 eKuiper 里创建任意多条规则，对采集的数据进行各种计算和处理。本文以两个采集规则为例，实现边缘采集到云端的场景。更多 eKuiper 的数据处理能力，请参考扩展阅读部分。

### 清洗数据到云端

假设 Neuron 中设置的两个tag 的真是含义为:
- tag1: decimal 表示的温度数据，实际温度应该除以10
- tag2: 整型的湿度数据。

本规则将采集的 neuron 数据换算为正确的精度，并重命名为有意义的名字。结果发送到云端的 MQTT 动态 topic `${nodeName}/${groupName}`中。 创建规则的 REST 命令如下。其中，规则名为 `ruleNAll`, 规则的 SQL 中对采集的值进行计算，并选取了`node_name` 和 `group_name` 这些元数据。在动作中，规则的结果发送到云端的 MQTT broker，而且 topic 为动态名字。根据前文配置，我们采集的 node_name 为 `modbus-plus-tcp-1`，group_name 为 `group-1`。因此，在 MQTT X 中，订阅 `modbus-plus-tcp-1/group-1` 主题即可得到计算的结果。

```shell
curl -X POST --location http://127.0.0.1:9081/rules \
    -H 'Content-Type: application/json' \
    -d '{
  "id": "ruleNAll",
  "sql": "SELECT node_name, group_name, values->tag1/10 as temperature, values->tag2 as humidity FROM neuronStream",
  "actions": [{
    "mqtt": {
      "server": "tcp://cloud.host:1883",
      "topic": "{{.node_name}}/{{.group_name}}",
      "sendSingle": true
    }
  }]
}'
```

打开 MQTT X，连接到云端 broker， 订阅 `modbus-plus-tcp-1/group-1` 主题，则可得到如下结果。由于采集频率为100ms 一次，此处收到的数据也是类似的频率。

![ruleNAll result](./ruleNAllResult.png)

在 Modbus TCP 模拟器修改数据，可得到变化的输出。

### 采集变化数据到云端

采集频率较高而数据变化频率较低时，用户通常会采集到大量的冗余重复数据，全部上传云端会占据大量带宽。eKuiper 提供了应用层的去重功能，可以创建规则采集变化数据。对上面的规则进行改造，增加过滤条件，仅当采集到的任一 tag 数据变化时才发送数据。新的规则变为：

```shell
curl -X POST --location http://127.0.0.1:9081/rules \
    -H 'Content-Type: application/json' \
    -d '{
  "id": "ruleChange",
  "sql": "SELECT node_name, group_name, values->tag1/10 as temperature, values->tag2 as humidity FROM neuronStream WHERE HAD_CHANGED(true, values->tag1, values->tag2)",
  "actions": [{
    "mqtt": {
      "server": "tcp://cloud.host:1883",
      "topic": "changed/{{.node_name}}/{{.group_name}}",
      "sendSingle": true
    }
  }]
}'
```

打开 MQTT X，连接到云端 broker， 订阅 `changed/modbus-plus-tcp-1/group-1` 主题，收到数据的频率大大降低。在 Modbus TCP 模拟器修改数据才可收到新的数据。

## 通过 Neuron 控制设备

得益于 neuron sink 组件，eKuiper 可以在数据处理后通过 neuron 控制设备。在下面的规则中，eKuiper 接收 MQTT 的指令，对 neuron 进行动态的反控。

假设有个应用场景，用户通过往云端的 MQTT 服务器的某个主题发送控制指令来对部署在边缘端的设备进行控制操作，比如设定目标设备的期望的温度。首先，我们在 eKuiper 中需要创建一个 MQTT 流，用于接收从别的应用发到 `command` MQTT 主题的指令。

```shell
curl -X POST --location "http://127.0.0.1:9081/streams" \
    -H 'Content-Type: application/json' \
    -d '{"sql":"CREATE STREAM mqttCommand() WITH (TYPE=\"mqtt\",SHARED=\"TRUE\",DATASOURCE=\"command\");"}'
```

接着，我们创建一个规则，读取来自该 MQTT 流的数据，并根据规则通过 Neuron 写入数据。与前文相同，假设 tag1 为温度传感器的 decimal 类型的读数。该规则读取 MQTT payload 中的 temperature 值并乘 10 之后作为 tag1 的值；使用 payload 中的 nodeName, groupName 字段作为写到 Neuron 中的动态 node 和 group 名。

```shell
curl -X POST --location http://127.0.0.1:9081/rules \
    -H 'Content-Type: application/json' \
    -d '{
  "id": "ruleCommand",
  "sql": "SELECT temperature * 10 as tag1, nodeName, groupName from mqttCommand",
  "actions": [{
    "log": {},
    "neuron": {
      "nodeName": "{{.nodeName}}",
      "groupName": "{{.groupName}}",
      "tags": [
        "tag1"
      ]
    }
  }]
}'
```

规则运行之后，打开 MQTT X，向 `command` 主题写入如下格式的JSON 串。需要注意的是，应当确保 node 和 group 在 neuron 中已创建。在本教程的配置中，只创建了 modbus-plus-tcp-1 和 group-1。

```json
{
  "nodeName": "modbus-plus-tcp-1",
  "temperature": 24,
  "groupName": "group-1"
}
```

打开 Neuron 的数据监控，可见 tag1 数据更改为 240，表明反控成功。

![data monitor after control](./data_monitor.png)

同时，前文创建的两个规则应该采集到新的数值。

## 扩展阅读

本教程使用到了 Neuron source 和 sink 的一部分功能，以及一部分流式计算的场景。

- 详细了解 Neuron 流入数据格式，请阅读[Neuron Source 参考](../../guide/sources/builtin/neuron.md)。
- 详细了解 Neuron 反控的相关参数，请阅读[Neuron Sink 参考](../../guide/sinks/builtin/neuron.md)。
- 了解 eKuiper 的[概念和基本使用场景](../../concepts/ekuiper.md)。
- 了解[规则的组成和参数](../../guide/rules/overview.md)。
- [eKuiper 管理控制台的使用](../../operation/manager-ui/overview.md#开始使用)。




