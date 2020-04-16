# EdgeX 消息总线目标

该目标用于将消息发送到 EdgeX 消息总线上。

| name        | Optional | Description                                                  |
| ----------- | -------- | ------------------------------------------------------------ |
| protocol    | true     | 如未指定，使用缺省值 ``tcp``.                                |
| host        | true     | 消息总线目标主机地址，使用缺省值 ``*``.                      |
| port        | true     | 消息总线端口号。 如未指定，使用缺省值 ``5563``.              |
| topic       | true     | 发布的主题名称，如未指定，使用缺省值 ``events``.             |
| contentType | true     | 发布消息的内容类型，如未指定，使用缺省值 ``application/json``. |
| metadata    | true     | 该属性为一个字段名称，该字段是 SQL SELECT 子句的一个字段名称，这个字段应该类似于 ``meta(*) AS xxx`` ，用于选出消息中所有的 EdgeX 元数据. |
| deviceName  | true     | 允许用户指定设备名称，该名称将作为从 Kuiper 中发送出来的 Event 结构体的设备名称. |
| type          | true     | 消息总线类型，目前支持两种类型的消息总线， ``zero`` 或者 ``mqtt``，其中 ``zero`` 为缺省类型。 |
| optional      | true     | 如果指定了 ``mqtt`` 消息总线，那么还可以指定一下可选的值。请参考以下可选的支持的配置类型。 |

请注意，所有在可选的配置项里指定的值都必须为**<u>字符类型</u>**，因此这里出现的所有的配置应该是字符类型的 - 例如 ``KeepAlive: "5000"``。以下为支持的可选的配置列表，您可以参考 MQTT 协议规范来获取更详尽的信息。

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

### 发布结果到 EdgeX 消息总线，而不保留原有的元数据
在此情况下，原有的元数据 (例如``Events`` 结构体中的 ``id, pushed, created, modified, origin``，以及``Reading`` 结构体中的  ``id, created, modified, origin, pushed, device`` 不会被保留)。Kuiper 在此情况下作为 EdgeX 的一个单独微服务，它有自己的 ``device name``。 提供了属性 ``deviceName``， 该属性允许用户指定 Kuiper 的设备名称。如下所示，

1) 从 EdgeX 消息总线上的 ``events`` 主题上收到的消息，

```
{
  "Device": "demo", "Created": 000, …
  "readings": 
  [
     {"Name": "Temperature", value: "30", "Created":123 …},
     {"Name": "Humidity", value: "20", "Created":456 …}
  ]
}
```
2) 使用如下的规则，并且在 ``edgex`` action 中给属性 ``deviceName`` 指定 ``kuiper``。

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
        "contentType": "application/json"
      }
    }
  ]
}
```
3) 发送到 EdgeX 消息总线上的数据。

```
{
  "Device": "kuiper", "Created": 0, …
  "readings": 
  [
     {"Name": "t1", value: "90" , "Created": 0 …},
     {"Name": "humidity", value: "20" , "Created": 0 …}
  ]
}
```
请注意，
- Event 结构体中的设备名称( `` Device``)变成了 ``kuiper``
- ``Events and Readings`` 结构体中的数据被更新为新的值. 字段 ``Created`` 被 Kuiper 更新为新的值 (这里为 ``0``).

### 发布结果到 EdgeX 消息总线，并保留原有的元数据
但是在某些场景中，你可能需要保留原来的元数据。比如保留发送到 Kuiper 的设备名称，在本例中为 ``demo``， 还有 reading 数组中的其它元数据。在此情况下，Kuiper 更像是一个过滤器 - 将不关心的数据过滤掉，但是依然保留原有的数据。

参考以下的例子，

1) 从 EdgeX 消息总线上的 ``events`` 主题上收到的消息，

```
{
  "Device": "demo", "Created": 000, …
  "readings": 
  [
     {"Name": "Temperature", value: "30", "Created":123 …},
     {"Name": "Humidity", value: "20", "Created":456 …}
  ]
}
```
2) 使用如下规则，在``edgex`` action 中，为 ``metadata`` 指定值 ``edgex_meta`` 。

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
- 用户需要在 SQL 子句中加 ``meta(*) AS edgex_meta`` ，函数 ``meta(*)`` 返回所有的元数据。
- 在 ``edgex`` action里， 属性 ``metadata`` 指定值 ``edgex_meta`` 。该属性指定哪个字段包含了元数据。

3) 发送给 EdgeX 消息总线的数据

```
{
  "Device": "demo", "Created": 000, …
  "readings": 
  [
     {"Name": "t1", value: "90" , "Created": 0 …},
     {"Name": "humidity", value: "20", "Created":456 …}
  ]
}
```
请注意，
- ``Events`` 结构体的元数据依然保留，例如 ``Device`` & ``Created``.
- 对于在原有消息中可以找到的 reading，元数据将继续保留。 比如 ``humidity`` 的元数据就是从 EdgeX 消息总线里接收到的``原值 - 或者说是旧值``。
- 对于在原有消息中无法找到的 reading，元数据将不会被设置。如例子中的``t1`` 的元数据被设置为 Kuiper 产生的缺省值。
- 如果你的 SQL 包含了聚合函数，那保留原有的元数据就没有意义，但是 Kuiper 还是会使用时间窗口中的某一条记录的元数据。例如，在下面的 SQL 里，
```SELECT avg(temperature) AS temperature, meta(*) AS edgex_meta FROM ... GROUP BY TUMBLINGWINDOW(ss, 10)```. 
这种情况下，在时间窗口中可能有几条数据，Kuiper 会使用窗口中的第一条数据的元数据来填充 ``temperature`` 的元数据。

## 结果发布到 MQTT 消息总线

以下是将分析结果发送到 MQTT 消息总线的规则，请注意在``optional`` 中是如何指定 ``ClientId`` 的。

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

