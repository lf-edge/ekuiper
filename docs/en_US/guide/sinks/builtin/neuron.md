# Neuron action

The action is used to publish result to the local neuron instance. Notice that, the sink is bound to the local neuron only which must be able to communicate through nanomsg ipc protocol without network. In the eKuiper side, all neuron source and sink instances share the same connection. Notice that, the dial to neuron is async which will run in the background and always redial when the previous attempt fails, which means that the rule using neuron sink will not see an error even when neuron is down. Additionally, there is a send queue of 128 messages in the nanomsg client, thus the neuron sink can have 128 messages send out even when neuron is down. The rule will only start to get timeout errors after 128 queued messages. Once the connection is restored, the queued messages will be sent out automatically.

| Property name | Optional | Description                                                                                                                             |
|---------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------|
| groupName     | true     | The neuron group to be sent to. Allow to use template as a dynamic property. It is required when using non raw mode.                    |
| nodeName      | true     | The neuron node to be sent to. Allow to use template as a dynamic property. It is required when using non raw mode.                     |
| tags          | true     | The field names to be sent to neuron as a tag. If not specified, all result fields will be sent.                                        |
| raw           | true     | Default to false. Whether to convert the data to neuron format by this sink or just publish the json or data template converted result. |

Other common sink properties are supported. Please refer to the [sink common properties](../overview.md#common-properties) for more information.

## Examples

Assume the sink receive result map like:

```json
{
  "temperature": 25.2,
  "humidity": 72,
  "status": "green",
  "node": "myNode"
}
```

### Send specify tags to neuron

Below is a sample neuron action configuration. In which, raw is false so the sink will convert the result map into neuron's default format. The `tags` specify the tag names to be sent.

```json
{
  "neuron": {
    "groupName": "group1",
    "nodeName": "node1",
    "tags": ["temperature","humidity"]
  }
}
```

This will send two tags temperature and humidity in group1, node1.

### Send all keys as tags to neuron

This configuration does not specify `tags` property, thus it will send all the fields as tags.

```json
{
  "neuron": {
    "groupName": "group1",
    "nodeName": "node1"
  }
}
```

This will send four tags temperature, humidity, status and node in group1, node1.

### Send to dynamic node value

In the configuration, the `nodeName` property is a template which will retrieve the value of `node` field in the result map.

```json
{
  "neuron": {
    "groupName": "group1",
    "nodeName": "{{.node}}",
    "tags": ["temperature","humidity"]
  }
}
```

Given this result, it will send end two tags temperature and humidity in group1, myNode.

### Send with raw content

Below is another sample to publish data directly to neuron by the data template converted string. The raw is set, the format will be controlled by the data template.

```json
{
  "neuron": {
    "raw": true,
    "dataTemplate": "your template here"
  }
}
```