# Neuron 动作

该动作用于将结果发送到本地的 neuron 实例中以反控 neuron。需要注意的是，该动作与本地的 neuron 绑定，因为通信是通过 nanomsg 的 ipc 协议进行，无法通过网络通信。在 eKuiper 端，所有的 neuron 源和动作共享同一个全局 neuron 连接。需要注意的是，拨号到 Neuron 是异步的，它将在后台运行，不断重拨直到连接成功。这意味着使用 Neuron sink 的规则即使在 Neuron 停机时也不会看到错误。此外，在 nanomsg 消息客户端中有一个 128 条消息的发送队列，因此即使在 Neuron 停机时，Neuron sink 也能有 128 条消息发送出去。规则只有在 128 条消息队列满之后才会开始出现超时错误。一旦连接恢复，队列中的消息就会自动发送出去。

| 属性名称      | 是否可选 | 描述                                                                   |
|-----------|------|----------------------------------------------------------------------|
| groupName | 是    | 发送到 neuron 的组名，值可以为动态参数模板。使用非 raw 模式时必须配置此选项。                        |
| nodeName  | 是    | 发送到 neuron 的节点名，值可以为动态参数模板。使用非 raw 模式时必须配置次选项。                       |
| tags      | 是    | 发送到 neuron 的标签名列表。如果未设置，则结果中的所有列都会作为标签发送。                            |
| raw       | 是    | 默认为 false。是否使用原始字符串格式（json或者经过数据模板转换的字符串）。若为否，则会自动将结果转换为 neuron 的格式。 |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

你可以通过 api 的方式提前检查对应 sink 端点的连通性: [连通性检查](../../../api/restapi/connection.md#连通性检查)

## 示例

假设接收到的结果如下所示：

```json
{
  "temperature": 25.2,
  "humidity": 72,
  "status": "green",
  "node": "myNode"
}
```

### 发送选定的标签

以下的示例 neuron 配置中，`raw` 参数为空，因此该动作将根据用户配置的其他参数将结果转换为 neuron 的默认格式。`tags` 参数指定了需要发送的标签的名字。

```json
{
  "neuron": {
    "groupName": "group1",
    "nodeName": "node1",
    "tags": ["temperature","humidity"]
  }
}
```

这个配置将发送两个标签 temperature 和 humidity 到 group1 组 node1 节点。

### 发送所有列

以下的配置中没有指定 `tags` 参数，因此所有结果中的列将作为标签发送。

```json
{
  "neuron": {
    "groupName": "group1",
    "nodeName": "node1"
  }
}
```

这个配置将发送四个标签 temperature， humidity， status 和 node 到 group1 组 node1 节点。

### 发送到动态的节点

在此配置中，`nodeName` 设置为一个数据模板，从结果里提取 `node` 列的值作为发送的节点名。

```json
{
  "neuron": {
    "groupName": "group1",
    "nodeName": "{{.node}}",
    "tags": ["temperature","humidity"]
  }
}
```

这个配置将发送两个标签 temperature 和 humidity 到 group1 组 myNode 节点。

### 发送原始字符串数据

以下配置中，数据模板转换后的字符串数据将直接发送到 neuron 中。

```json
{
  "neuron": {
    "raw": true,
    "dataTemplate": "your template here"
  }
}
```
