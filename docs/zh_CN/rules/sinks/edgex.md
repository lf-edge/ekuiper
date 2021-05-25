# EdgeX 消息总线目标

该目标用于将消息发送到 EdgeX 消息总线上。

**请注意，如果你使用的是 ZeorMQ 消息总线，那么该 sink 会创建一个新的 EdgeX 消息总线（绑定到 eKuiper 服务所运行的地址），而不是利用原来既有的消息总线（通常为 application 服务所暴露的地址和端口）。 **

**另外，如果你需要在别的主机上对你的端口可以进行访问，你需要在开始运行 eKuiper 服务之前，把端口号映射到主机上。**

| 名称        | 可选 | Description                                                  |
| ----------- | -------- | ------------------------------------------------------------ |
| protocol    | true     | 协议，如未指定，使用缺省值 `tcp`.                             |
| host        | 是    | 消息总线主机地址，使用缺省值 `*`.                      |
| port        | 是    | 消息总线端口号。 如未指定，使用缺省值 `5563`.              |
| topic       | 是    | 发布的主题名称，如未指定，使用缺省值 `events`.             |
| contentType | 是    | 发布消息的内容类型，如未指定，使用缺省值 `application/json`. |
| metadata    | 是    | 该属性为一个字段名称，该字段是 SQL SELECT 子句的一个字段名称，这个字段应该类似于 `meta(*) AS xxx` ，用于选出消息中所有的 EdgeX 元数据. |
| deviceName  | 是    | 允许用户指定设备名称，该名称将作为从 eKuiper 中发送出来的 Event 结构体的设备名称. |
| profileName  | 是    | 允许用户指定设备名称，该名称将作为从 Kuiper 中发送出来的 Event 结构体的 profile 名称. |
| type          | 是    | 消息总线类型，目前支持两种类型的消息总线， `zero` 或者 `mqtt`，其中 `zero` 为缺省类型。 |
| optional      | 是    | 如果指定了 `mqtt` 消息总线，那么还可以指定一下可选的值。请参考以下可选的支持的配置类型。 |

以下为支持的可选的配置列表，您可以参考 MQTT 协议规范来获取更详尽的信息。

- optional
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

## 例子

### 发布结果到  EdgeX 消息总线，而不保留原有的元数据
在此情况下，原有的元数据 (例如 `Events` 结构体中的 `id, deviceName, profileName, sourceName, origin, tags`，以及`Reading` 结构体中的  `id, deviceName, profileName, origin, valueType` 不会被保留)。eKuiper 在此情况下作为 EdgeX 的一个单独微服务，它有自己的 `device name`， `profile name` 和 `source name`。 提供了属性 `deviceName` 和 `profileName`， 这两个属性允许用户指定 eKuiper 的设备名称和 profile 名称。而 `sourceName` 默认为 `topic` 属性的值。如下所示，

1) 从 EdgeX 消息总线上的 `events` 主题上收到的消息，

```
{
  "DeviceName": "demo", "Origin": 000, …
  "readings": 
  [
     {"ResourceName": "Temperature", value: "30", "Origin":123 …},
     {"ResourceName": "Humidity", value: "20", "Origin":456 …}
  ]
}
```
2) 使用如下的规则，并且在 `edgex` action 中给属性 `deviceName` 指定 ``kuiper``，属性 `profileName` 指定 ``kuiperProfile``。

```json
{
  "id": "rule1",
  "sql": "SELECT temperature * 3 AS t1, humidity FROM events",
  "actions": [
    {
      "edgex": {
        "protocol": "tcp",
        "host": "*",
        "port": 5571,
        "topic": "application",
        "deviceName": "kuiper",
        "profileName": "kuiperProfile",
        "contentType": "application/json"
      }
    }
  ]
}
```
3) 发送到 EdgeX 消息总线上的数据。

```
{
  "DeviceName": "kuiper", "ProfileName": "kuiperProfile",  "Origin": 0, …
  "readings": 
  [
     {"ResourceName": "t1", value: "90", "Origin": 0 …},
     {"ResourceName": "humidity", value: "20" , "Origin": 0 …}
  ]
}
```
请注意，
- Event 结构体中的设备名称( ``DeviceName``)变成了 `kuiper`，profile 名称( ``ProfileName``)变成了 `kuiperProfile`
- `Events and Readings` 结构体中的数据被更新为新的值。 字段 `Origin` 被 eKuiper 更新为新的值 (这里为 `0`).

### 发布结果到  EdgeX 消息总线，并保留原有的元数据
但是在某些场景中，你可能需要保留原来的元数据。比如保留发送到 eKuiper 的设备名称，在本例中为 `demo`， 还有 reading 数组中的其它元数据。在此情况下，eKuiper 更像是一个过滤器 - 将不关心的数据过滤掉，但是依然保留原有的数据。

参考以下的例子，

1) 从 EdgeX 消息总线上的 `events` 主题上收到的消息，

```
{
  "DeviceName": "demo", "Origin": 000, …
  "readings": 
  [
     {"ResourceName": "Temperature", value: "30", "Origin":123 …},
     {"ResourceName": "Humidity", value: "20", "Origin":456 …}
  ]
}
```
2) 使用如下规则，在`edgex` action 中，为 `metadata` 指定值 `edgex_meta` 。

```json
{
  "id": "rule1",
  "sql": "SELECT meta(*) AS edgex_meta, temperature * 3 AS t1, humidity FROM events WHERE temperature > 30",
  "actions": [
    {
      "edgex": {
        "protocol": "tcp",
        "host": "*",
        "port": 5571,
        "topic": "application",
        "metadata": "edgex_meta",
        "contentType": "application/json"
      }
    }
  ]
}
```
请注意，
- 用户需要在 SQL 子句中加 `meta(*) AS edgex_meta` ，函数 `meta(*)` 返回所有的元数据。
- 在 `edgex` action里， 属性 `metadata` 指定值 `edgex_meta` 。该属性指定哪个字段包含了元数据。

3) 发送给 EdgeX 消息总线的数据

```
{
  "DeviceName": "demo", "Origin": 000, …
  "readings": 
  [
     {"ResourceName": "t1", value: "90" , "Origin": 0 …},
     {"ResourceName": "humidity", value: "20", "Origin":456 …}
  ]
}
```
请注意，
- `Events` 结构体的元数据依然保留，例如 `DeviceName` & `Origin`.
- 对于在原有消息中可以找到的 reading，元数据将继续保留。 比如 `humidity` 的元数据就是从 EdgeX 消息总线里接收到的`原值 - 或者说是旧值`。
- 对于在原有消息中无法找到的 reading，元数据将不会被设置。如例子中的 `t1` 的元数据被设置为 eKuiper 产生的缺省值。
- 如果你的 SQL 包含了聚合函数，那保留原有的元数据就没有意义，但是 eKuiper 还是会使用时间窗口中的某一条记录的元数据。例如，在下面的 SQL 里，
```SELECT avg(temperature) AS temperature, meta(*) AS edgex_meta FROM ... GROUP BY TUMBLINGWINDOW(ss, 10)```. 
这种情况下，在时间窗口中可能有几条数据，eKuiper 会使用窗口中的第一条数据的元数据来填充 `temperature` 的元数据。

## 结果发布到 MQTT 消息总线

以下是将分析结果发送到 MQTT 消息总线的规则，请注意在`optional` 中是如何指定 `ClientId` 的。

```json
{
  "id": "rule1",
  "sql": "SELECT meta(*) AS edgex_meta, temperature, humidity, humidity*2 as h1 FROM demo WHERE temperature = 20",
  "actions": [
    {
      "edgex": {
        "protocol": "tcp",
        "host": "127.0.0.1",
        "port": 1883,
        "topic": "result",
        "type": "mqtt",
        "metadata": "edgex_meta",
        "contentType": "application/json",
        "optional": {
        	"ClientId": "edgex_message_bus_001"
        }
      }
    },
    {
      "log":{}
    }
  ]
}
```

