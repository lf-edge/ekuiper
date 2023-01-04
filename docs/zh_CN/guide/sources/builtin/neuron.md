# Neuron 源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

Neuron 源订阅本地 neuron 实例的消息。需要注意的是，该源仅可用于本地的 neuron，因为与 neuron 的通信基于 nanomsg ipc 协议，无法通过网络进行。在 eKuiper 端，所有 neuron 源和动作共享同一个 neuron 连接。需要注意的是，拨号到 Neuron 是异步的，它将在后台运行，不断重拨直到连接成功。这意味着使用 Neuron sink 的规则即使在 Neuron 停机时也不会看到错误。用户调试时，可查看规则状态，注意消息流入数量是否正常。

Neuron 发过来的消息为固定的 json 格式，如下所示： 

```json
{
  "timestamp": 1646125996000,
  "node_name": "node1", 
  "group_name": "group1",
  "values": {
    "tag_name1": 11.22,
    "tag_name2": "string"
  },
  "errors": {
    "tag_name3": 122
  }
}
```

该源没有可配置属性。使用时仅需设置 `TYPE` 属性，示例如下：

```text
CREATE STREAM table1 () WITH (FORMAT="json", TYPE="neuron");
```