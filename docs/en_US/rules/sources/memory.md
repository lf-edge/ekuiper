# Memory Source

Memory source is provided to consume events produced by the [memory sink](../sinks/memory.md) through topics. The topic is like pubsub topic such as mqtt, so that there could be multiple memory sinks which publish to the same topic and multiple memory sources which subscribe to the same topic. The typical usage for memory action is to form [rule pipelines](../rule_pipeline.md).

There is no configuration properties. The topic is specified by the stream data source property like below examples:

```text
CREATE TABLE table1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="devices/result", FORMAT="json", TYPE="memory");
```

## Topic Wildcard

Similar to mqtt topic, memory source also supports topic wildcards. Currently, there are two wildcards supported.

**+** : Single level wildcard replaces one topic level. 
**#**: Multi level wildcard covers multiple topic levels, and it can only be used at the end.

Examples:
1. `home/device1/+/sensor1`
2. `home/device1/#`