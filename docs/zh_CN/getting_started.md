

## 下载和安装

通过 <https://github.com/lf-edge/ekuiper/releases>  获取安装包.

### zip、tar.gz 压缩包

解压缩 eKuiper

```sh
$ unzip kuiper-$VERISON-$OS-$ARCH.zip
or
$ tar -xzf kuiper-$VERISON-$OS-$ARCH.zip
```

运行 ``bin/kuiperd`` 以启动 eKuiper 服务器

```sh
$ bin/kuiperd
```
 您应该会看到一条成功的消息：`Serving Rule server on port 20498`

eKuiper 的目录结构如下:

```
eKuiper_installed_dir
  bin
    kuiperd
    kuiper
  etc
    mqtt_source.yaml
    ...
  data
    ...
  plugins
    ...
  log
    ...
```


#### deb、rpm 安装包

使用相关命令安装 eKuiper

```sh
$ sudo dpkg -i kuiper_$VERSION_$ARCH.deb
or
$ sudo rpm -ivh kuiper-$VERSION-1.el7.rpm
```

运行 `kuiperd` 以启动 eKuiper 服务器

```sh
$ sudo kuiperd
```
 您应该会看到一条成功的消息：`Serving Rule server on port 20498`

 kuiper 也支持 systemctl 启动

 ```sh
 $ sudo systemctl start kuiper
 ```

eKuiper 的目录结构如下:

```
/usr/lib/kuiper/bin
  kuiperd
  kuiper
/etc/kuiper
  mqtt_source.yaml
  ...
/var/lib/kuiper/data
  ...
/var/lib/kuiper/plugins
  ...
/var/log/kuiper
   ...
```

## 运行的第一个规则流

eKuiper 规则由一个 SQL 和多个操作组成。 eKuiper SQL 是一种易于使用的类 SQL 语言，用于确定规则流的逻辑。 通过命令行提供规则，规则流将在规则引擎中创建并连续运行。用户之后可以通过命令行管理规则。

eKuiper 具有许多用于复杂分析的内置函数和扩展，您可以访问 [eKuiper SQL 参考](sqls/overview.md)获取有关语法和其功能的更多信息。

让我们考虑一个示例场景：我们正在通过 MQTT 服务从传感器接收温度和湿度记录，并且当温度在一个时间窗口中大于30摄氏度时，我们希望发出警报。 我们可以使用以下几个步骤为上述场景编写 eKuiper 规则。

### 先决条件

我们假设已经有一个 MQTT 消息服务器作为 eKuiper 服务器的数据源。 如果您没有，建议使用 EMQ X。 请按照 [EMQ Broker 安装指南](https://docs.emqx.cn/cn/broker/latest/getting-started/install.html)设置 MQTT 消息服务器。

### 定义输入流

流需要具有一个名称和一个架构，以定义每个传入事件应包含的数据。 对于这种情况，我们将使用 MQTT 源应对温度事件。 输入流可以通过 SQL 语言定义。

我们创建一个名为 ``demo`` 的流，该流使用 ``DATASOURCE`` 属性中指定的 MQTT ``demo`` 主题。
```sh
$ bin/kuiper create stream demo '(temperature float, humidity bigint) WITH (FORMAT="JSON", DATASOURCE="demo")'
```
MQTT 源将通过`tcp://localhost:1883`连接到 MQTT 消息服务器，如果您的 MQTT 消息服务器位于别的位置，请在`etc/mqtt_source.yaml`中进行指定。 您可以通过修改如下配置文件来更改配置。

```yaml
default:
  qos: 1
  sharedsubscription: true
  servers: [tcp://127.0.0.1:1883]
```

您可以使用``kuiper show streams`` 命令来查看是否创建了 ``demo`` 流。

### 通过查询工具测试流

现在已经创建了流，可以通过 ``kuiper query`` 命令对其进行测试。键入``kuiper query``后，显示 ``kuiper``提示符。

```sh
$ bin/kuiper query
kuiper > 
```

在 ``kuiper``提示符下，您可以键入 SQL 并根据流验证 SQL。

```sh
kuiper > select count(*), avg(humidity) as avg_hum, max(humidity) as max_hum from demo where temperature > 30 group by TUMBLINGWINDOW(ss, 5);

query is submit successfully.
```

现在，如果有任何数据发布到位于``tcp://127.0.0.1:1883``的 MQTT 服务器，那么它打印如下消息。

```
kuiper > [{"avg_hum":41,"count":4,"max_hum":91}]
[{"avg_hum":62,"count":5,"max_hum":96}]
[{"avg_hum":36,"count":3,"max_hum":63}]
[{"avg_hum":48,"count":3,"max_hum":71}]
[{"avg_hum":40,"count":3,"max_hum":69}]
[{"avg_hum":44,"count":4,"max_hum":57}]
[{"avg_hum":42,"count":3,"max_hum":74}]
[{"avg_hum":53,"count":3,"max_hum":81}]
...
```

您可以按 ``ctrl + c`` 键中断查询，如果检测到客户端与查询断开连接，服务器将终止流传输。 以下是服务器上的日志打印。

```
...
time="2019-09-09T21:46:54+08:00" level=info msg="The client seems no longer fetch the query result, stop the query now."
time="2019-09-09T21:46:54+08:00" level=info msg="stop the query."
...
```

### 编写规则

作为规则的一部分，我们需要指定以下内容：
* 规则名称：规则的 ID。 它必须是唯一的
* sql：针对规则运行的查询
* 动作：规则的输出动作

我们可以运行 ``kuiper rule`` 命令来创建规则并在文件中指定规则定义

```sh
$ bin/kuiper create rule ruleDemo -f myRule
```
`myRule`文件的内容。 对于在1分钟内滚动时间窗口中的平均温度大于30的事件，它将打印到日志中。

```json
{
    "sql": "SELECT temperature from demo where temperature > 30",
    "actions": [{
        "log":  {}
    }]
}
```
您应该在流日志中看到一条成功的消息`` rule ruleDemo created``。 现在，规则已经建立并开始运行。

### 测试规则
现在，规则引擎已准备就绪，可以接收来自 MQTT ``demo`` 主题的事件。 要对其进行测试，只需使用 MQTT 客户端将消息发布到 ``demo`` 主题即可。 该消息应为 json 格式，如下所示：

```json
{"temperature":31.2, "humidity": 77}
```

检查位于“ `log/stream.log`”的流日志，您会看到已过滤的数据被打印出来。 另外，如果您发送以下消息，则它不符合 SQL 条件，并且该消息将被过滤。

```json
{"temperature":29, "humidity": 80}
```

### 管理规则
您可以使用命令行暂停规则一段时间，然后重新启动规则和其他管理工作。 规则名称是规则的标识符。 查看[规则管理 CLI](rules/overview.md) 以了解详细信息

```sh
$ bin/kuiper stop rule ruleDemo
```


请参考以下主题，以获取有关使用 eKuiper 的指导。
- [命令行界面工具 CLI](./cli/overview.md)
- [eKuiper SQL 参考](./sqls/overview.md)
- [规则](./rules/overview.md)
- [扩展 eKuiper](./extension/overview.md)
- [插件](./plugins/overview.md)
