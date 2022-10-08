# MQTT源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper 为 MQTT 源流提供了内置支持，流可以订阅来自 MQTT 代理的消息并输入eKuiper 处理管道。 MQTT 源的配置文件位于 `$ekuiper/etc/mqtt_source.yaml`。 以下是文件格式。

```yaml
#全局MQTT配置
default:
  qos: 1
  server: "tcp://127.0.0.1:1883"
  #username: user1
  #password: password
  #certificationPath: /var/kuiper/xyz-certificate.pem
  #privateKeyPath: /var/kuiper/xyz-private.pem.key
  kubeedgeVersion: "1.0"
  kubeedgeModelFile: "mqtt_model.json"


#重载全局配置
demo: #Conf_key
  qos: 0
  server: "tcp://10.211.55.6:1883"
```

## 全局 MQTT 配置

用户可以在此处指定全局 MQTT 设置。`default` 部分中指定的配置项将用作所有MQTT 连接的默认设置。

### qos

默认订阅QoS级别。

### server

MQTT 消息代理的服务器。

### username

MQTT 连接用户名。

### password

MQTT 连接密码。

### protocolVersion

MQTT 协议版本。3.1 (也被称为 MQTT 3) 或者 3.1.1 (也被称为 MQTT 4)。 如果未指定，缺省值为 3.1。

### clientid

MQTT 连接的客户端 ID。 如果未指定，将使用一个 uuid。

### certificationPath

证书路径。可以为绝对路径，也可以为相对路径。如果指定的是相对路径，那么父目录为执行 `kuiperd` 命令的路径。比如，如果你在 `/var/kuiper` 中运行 `bin/kuiperd` ，那么父目录为 `/var/kuiper`; 如果运行从`/var/kuiper/bin`中运行`./kuiperd`，那么父目录为 `/var/kuiper/bin`。 比如  `d3807d9fa5-certificate.pem`。

### privateKeyPath

私钥路径。可以为绝对路径，也可以为相对路径。更详细的信息，请参考 `certificationPath`，比如 `d3807d9fa5-private.pem.key`。

### rootCaPath

根证书路径。可以为绝对路径，也可以为相对路径。

### insecureSkipVerify

如果 InsecureSkipVerify 设置为 true, TLS接受服务器提供的任何证书以及该证书中的任何主机名。 在这种模式下，TLS容易受到中间人攻击。默认值为false。配置项只能用于TLS连接

### connectionSelector

重用 MQTT 源连接。连接配置信息位于 `connections/connection.yaml`.
```yaml
mqtt:
  localConnection: #connection key
    server: "tcp://127.0.0.1:1883"
    username: ekuiper
    password: password
    #certificationPath: /var/kuiper/xyz-certificate.pem
    #privateKeyPath: /var/kuiper/xyz-private.pem.ke
    #insecureSkipVerify: false
    #protocolVersion: 3
    clientid: ekuiper
  cloudConnection: #connection key
    server: "tcp://broker.emqx.io:1883"
    username: user1
    password: password
    #certificationPath: /var/kuiper/xyz-certificate.pem
    #privateKeyPath: /var/kuiper/xyz-private.pem.ke
    #insecureSkipVerify: false
    #protocolVersion: 3
```
对于 MQTT 连接，这里有两个配置组。用户应该使用 `mqtt.localConnection` 或者 `mqtt.cloudConnection` 来作为参数。举例如下：
```yaml
#Global MQTT configurations
default:
  qos: 1
  server: "tcp://127.0.0.1:1883"
  #username: user1
  #password: password
  #certificationPath: /var/kuiper/xyz-certificate.pem
  #privateKeyPath: /var/kuiper/xyz-private.pem.key
  connectionSelector: mqtt.localConnection
```
*注意*: 相应配置组一旦指定 connectionSelector 参数，所有关于连接的参数都会被忽略. 上面例子中，`` server: "tcp://127.0.0.1:1883"`` 会被忽略。

### bufferLength

指定最大缓存消息数目。该参数主要用于防止内存溢出。实际内存用量会根据当前缓存消息数目动态变化。增大该参数不会增加初始内存分配量，因此设置较大的数值是安全的。该参数默认值为102400；如果每条消息为100字节，则默认情况下，缓存最大占用内存量为102400 * 100B ~= 10MB. 

### kubeedgeVersion

kubeedge 版本号，不同的版本号对应的文件内容不同。

### kubeedgeModelFile

kubeedge 模版文件名，文件指定放在 etc/sources 文件夹中，样例格式如下：

```json
{
	"deviceModels": [{
		"name": "device1",
		"properties": [{
			"name": "temperature",
			"dataType": "int"
		}, {
			"name": "temperature-enable",
			"dataType": "string"
		}]
	}]
}
```

#### deviceModels.name

设备名称,与订阅主题中的字段匹配，位于第三和第四个“/”之间的内容。例如：$ke/events/device/device1/data/update

#### properties.name

字段名称

#### properties.dataType

期望的字段类型

## 重载默认设置

如果您有一个特定连接需要重载默认设置，则可以创建一个自定义模块。 在上一个示例中，我们创建一个名为 `demo` 的特定设置。 然后，您可以在创建流定义时使用选项 `CONF_KEY` 指定配置（有关更多信息，请参见 [stream specs](../../../sqls/streams.md) ）。

**示例**

```
demo (
		...
	) WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo");
```

这些特定设置使用的配置键与 `default` 设置中的配置键相同，在特定设置中指定的任何值都将覆盖 `default` 部分中的值。

## 在多个配置块中引用同一个 connectionSelector

如下所示，用户创建了两个配置项
```yaml
#Override the global configurations
demo_conf: #Conf_key
  qos: 0
  connectionSelector: mqtt.localConnection 
  servers: [tcp://10.211.55.6:1883, tcp://127.0.0.1]

#Override the global configurations
demo2_conf: #Conf_key
  qos: 0
  connentionSelector: mqtt.localConnection
  servers: [tcp://10.211.55.6:1883, tcp://127.0.0.1]
```
基于以上配置，创建了两个数据流

```
demo (
		...
	) WITH (DATASOURCE="test/", FORMAT="JSON", CONF_KEY="demo_conf");

demo2 (
		...
	) WITH (DATASOURCE="test2/", FORMAT="JSON", CONF_KEY="demo2_conf");

```

当相应的规则分别引用以上数据流时，规则之间的源部分将共享连接。
在这里 `DATASOURCE` 对应 mqtt 订阅的 topic, 配置项中的 `qos` 将用作订阅时的 `Qos`.
在这个例子中，`demo` 以 Qos 0 订阅 topic `test/`,`demo2` 以 Qos 0 订阅 topic `test2/`
值得注意的是如果两个规则订阅的 `topic` 完全一样而 `Qos` 不同，那么只会订阅一次并以首先启动的规则订阅为准。

## 迁移指南

从 1.5.0 开始，eKuiper 将 mqtt 源地址配置从 `servers` 更改为 `server`，用户只能配置一个 mqtt 源地址而不是一个地址数组。
使用之前版本并把 mqtt broker 作为数据源的用户，想要迁移到 1.5.0 或更高版本，需要确保 ``etc/mqtt_source.yaml`` 文件 ``server`` 的配置是正确的。
使用环境变量配置 mqtt 源地址的用户需要成功更改配置，假设其地址为 ``tcp://broker.emqx.io:1883``。他们需要将环境变量 从
``MQTT_SOURCE__DEFAULT__SERVERS=[tcp://broker.emqx.io:1883]`` 改为 ``MQTT_SOURCE__DEFAULT__SERVER="tcp://broker.emqx.io:1883"``