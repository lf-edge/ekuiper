# MQTT源

Kuiper 为 MQTT 源流提供了内置支持，流可以订阅来自 MQTT 代理的消息并输入Kuiper 处理管道。 MQTT 源的配置文件位于 `$kuiper/etc/mqtt_source.yaml`。 以下是文件格式。

```yaml
#全局MQTT配置
default:
  qos: 1
  sharedsubscription: true
  servers: [tcp://127.0.0.1:1883]
  concurrency: 1
  #username: user1
  #password: password
  #certificationPath: /var/kuiper/xyz-certificate.pem
  #privateKeyPath: /var/kuiper/xyz-private.pem.key


#重载全局配置
demo: #Conf_key
  qos: 0
  servers: [tcp://10.211.55.6:1883]
```

## 全局 MQTT 配置

用户可以在此处指定全局 MQTT 设置。`default` 部分中指定的配置项将用作所有MQTT 连接的默认设置。

### qos

默认订阅QoS级别。

### concurrency
设置运行的协程数，默认值为1。如果设置协程数大于1，必须使用共享订阅模式。

### sharedsubscription

是否使用共享订阅模式。 如果使用共享订阅模式，那么多个 Kuiper 进程可以进行负载平衡。

### servers

MQTT 消息代理的服务器列表。 当前，只能指定一个服务器。

### username

MQTT 连接用户名。如果指定了 `certificationPath`  或者 `privateKeyPath`，那么该项配置不会被使用。

### password

MQTT 连接密码。如果指定了 `certificationPath` 或者 `privateKeyPath`，那么该项配置不会被使用。

### certificationPath

证书路径。可以为绝对路径，也可以为相对路径。如果指定的是相对路径，那么父目录为执行 `server` 命令的路径。比如，如果你在 `/var/kuiper` 中运行 `bin/server` ，那么父目录为 `/var/kuiper`; 如果运行从`/var/kuiper/bin`中运行`./server`，那么父目录为 `/var/kuiper/bin`。 比如  `d3807d9fa5-certificate.pem`。

### privateKeyPath

私钥路径。可以为绝对路径，也可以为相对路径。更详细的信息，请参考 `certificationPath`，比如 `d3807d9fa5-private.pem.key`。

### bufferLength

指定最大缓存消息数目。该参数主要用于防止内存溢出。实际内存用量会根据当前缓存消息数目动态变化。增大该参数不会增加初始内存分配量，因此设置较大的数值是安全的。该参数默认值为102400；如果每条消息为100字节，则默认情况下，缓存最大占用内存量为102400 * 100B ~= 10MB. 

## 重载默认设置

如果您有一个特定连接需要重载默认设置，则可以创建一个自定义模块。 在上一个示例中，我们创建一个名为 `demo` 的特定设置。 然后，您可以在创建流定义时使用选项 `CONF_KEY` 指定配置（有关更多信息，请参见 [stream specs](../../sqls/streams.md) ）。

**示例**

```
demo (
		...
	) WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo");
```

这些特定设置使用的配置键与 `default` 设置中的配置键相同，在特定设置中指定的任何值都将覆盖 `default` 部分中的值。

