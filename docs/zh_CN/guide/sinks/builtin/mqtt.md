# MQTT动作

该操作用于将输出消息发布到 MQTT 服务器中。

| 属性名称               | 是否可选 | 说明                                                                                                                                                                                        |
|--------------------|------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| server             | 否    | MQTT  服务器地址，例如 `tcp://127.0.0.1:1883`                                                                                                                                                     |
| topic              | 否    | MQTT 主题，例如 `analysis/result` , 也可设置为动态属性，例如 `$.col`, 将会把结果中的 col 列的值作为主题                                                                                                                  |
| clientId           | 是    | MQTT 连接的客户端 ID。 如果未指定，将使用一个 uuid                                                                                                                                                          |
| protocolVersion    | 是    | MQTT 协议版本。3.1 (也被称为 MQTT 3) 或者 3.1.1 (也被称为 MQTT 4)。 如果未指定，缺省值为 3.1。                                                                                                                       |
| qos                | 是    | 消息转发的服务质量                                                                                                                                                                                 |
| username           | 是    | 连接用户名                                                                                                                                                                                     |
| password           | 是    | 连接密码                                                                                                                                                                                      |
| certificationPath  | 是    | 证书路径。可以为绝对路径，也可以为相对路径。如果指定的是相对路径，那么父目录为执行 `kuiperd` 命令的路径。比如，如果你在 `/var/kuiper` 中运行 `bin/kuiperd` ，那么父目录为 `/var/kuiper`; 如果运行从 `/var/kuiper/bin` 中运行`./kuiperd`，那么父目录为 `/var/kuiper/bin`。 |
| privateKeyPath     | 是    | 私钥路径。可以为绝对路径，也可以为相对路径，相对路径的用法与 `certificationPath` 类似。                                                                                                                                    |
| rootCaPath         | 是    | 根证书路径，用以验证服务器证书。可以为绝对路径，也可以为相对路径，相对路径的用法与 `certificationPath` 类似。                                                                                                                         |
| insecureSkipVerify | 是    | 如果 InsecureSkipVerify 设置为 `true`, TLS接受服务器提供的任何证书以及该证书中的任何主机名。 在这种模式下，TLS容易受到中间人攻击。默认值为 `false`。配置项只能用于TLS连接。                                                                              |
| retained           | 是    | 如果 retained 设置为 `true`,Broker会存储每个 Topic 的最后一条保留消息及其 Qos。默认值是 `false`                                                                                                                        |
| compression        | 是    | 使用指定的压缩方法压缩 Payload。当前支持 zlib, gzip, flate, zstd  算法。                                                                                                                                     |
| connectionSelector | 是    | 重用到 MQTT Broker 的连接，详细信息，[请参考](../../sources/builtin/mqtt.md#connectionselector)                                                                                                          |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

以下为使用 SAS 连接到 Azure IoT Hub 的样例。

```json
    {
      "mqtt": {
        "server": "ssl://xyz.azure-devices.net:8883",
        "topic": "devices/demo_001/messages/events/",
        "protocolVersion": "3.1.1",
        "qos": 1,
        "clientId": "demo_001",
        "username": "xyz.azure-devices.net/demo_001/?api-version=2018-06-30",
        "password": "SharedAccessSignature sr=*******************",
        "retained": false
      }
    }
```

以下为使用证书和私钥连接到 AWS IoT的另一个样例。

```json
    {
      "mqtt": {
        "server": "ssl://xyz-ats.iot.us-east-1.amazonaws.com:8883",
        "topic": "devices/result",
        "qos": 1,
        "clientId": "demo_001",
        "certificationPath": "keys/d3807d9fa5-certificate.pem",
        "privateKeyPath": "keys/d3807d9fa5-private.pem.key",
        "retained": false
      }
    }
```

## 动态主题

若结果数据中包含主题内容，可以将其作为主题属性，从而实现动态主题的需求。假设 SQL 选出的数据包含 `mytopic`, 则可以使用数据模板的语法将其设置为 `topic` 属性的值，如下所示：

```json
    {
      "mqtt": {
        "server": "ssl://xyz-ats.iot.us-east-1.amazonaws.com:8883",
        "topic": "{{.mytopic}}",
        "qos": 1,
        "clientId": "demo_001",
        "certificationPath": "keys/d3807d9fa5-certificate.pem",
        "privateKeyPath": "keys/d3807d9fa5-private.pem.key",
        "retained": false
      }
    }
```
