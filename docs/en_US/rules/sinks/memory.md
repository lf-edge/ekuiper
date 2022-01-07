# Memory action

The action is used to flush the result into an in-memory topic so that it can be consumed by the [memory source](../sources/memory.md). The topic is like pubsub topic such as mqtt, so that there could be multiple memory sinks which publish to the same topic and multiple memory sources which subscribe to the same topic. The typical usage for memory action is to form [rule pipelines](../rule_pipeline.md).

| Property name | Optional | Description                                    |
|---------------|----------|------------------------------------------------|
| topic         | false    | The in-memory topic, such as `analysis/result` |

Below is a sample memory action configuration:

```json
{
  "memory": {
    "topic": "devices/result"
  }
}
```

Below is another sample for dynamic topic action:

```json
{
  "memory": {
    "topic": "{{.topic}}"
  }
}
```