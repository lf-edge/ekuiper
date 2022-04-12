# Neuron 源

Neuron 源订阅本地 neuron 实例的消息。需要注意的是，该源仅可用于本地的 neuron，因为与 neuron 的通信基于 nanomsg ipc 协议，无法通过网络进行。在 eKuiper 端，所有 neuron 源和动作共享同一个 neuron 连接。Neuron 发过来的消息为固定的 json 格式，如下所示： 

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