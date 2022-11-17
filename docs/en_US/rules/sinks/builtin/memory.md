# Memory action

<span style="background:green;color:white">updatable</span>

The action is used to flush the result into an in-memory topic so that it can be consumed by the [memory source](../../sources/builtin/memory.md). The topic is like pubsub topic such as mqtt, so that there could be multiple memory sinks which publish to the same topic and multiple memory sources which subscribe to the same topic. The typical usage for memory action is to form [rule pipelines](../../rule_pipeline.md).

| Property name | Optional | Description                                                                                                        |
|---------------|----------|--------------------------------------------------------------------------------------------------------------------|
| topic         | false    | The in-memory topic, such as `analysis/result`                                                                     |
| rowkindField  | true     | Specify which field represents the action like insert or update. If not specified, all rows are default to insert. |

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

## Data Templates

::: v-pre
The data transfer between the memory action and the memory source is in internal format and is not coded or decoded for efficiency. Therefore, the format-related configuration items of the memory action are ignored, except for the data template. The memory action can support data templates to vary the result format, but the result of the data template must be in the object form of a JSON string, e.g. `"{\"key\":\"{{.key}}\"}"`. JSON strings in the form of arrays or non-JSON strings are not supported.
:::

## Updatable Sink

The memory sink support [updatable](../overview.md#updatable-sink). It is used to update the lookup table which subscribes to the same topic as the sink. A typical usage is to create a rule that use the updatable sink to accumulate the memory table. In below example, the data from stream alertStream will update the memory topic `alertVal`. The action verb is specified by the `action` field in the ingested data.

```json
{
  "id": "ruleUpdateAlert",
  "sql":"SELECT * FROM alertStream",
  "actions":[
    {
      "memory": {
        "keyField": "id",
        "rowkindField": "action",
        "topic": "alertVal",
        "sendSingle": true
      }
    }
  ]
}
```