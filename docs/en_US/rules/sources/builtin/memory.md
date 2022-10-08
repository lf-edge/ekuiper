# Memory Source

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>
<span style="background:green;color:white">lookup table source</span>

Memory source is provided to consume events produced by the [memory sink](../../sinks/builtin/memory.md) through topics. The topic is like pubsub topic such as mqtt, so that there could be multiple memory sinks which publish to the same topic and multiple memory sources which subscribe to the same topic. The typical usage for memory action is to form [rule pipelines](../../rule_pipeline.md). The data transfer between the memory action and the memory source is in internal format and is not coded or decoded for efficiency. Therefore, the `format` attribute of the memory source is ignored.

There is no configuration properties. The topic is specified by the stream data source property like below examples:

```text
CREATE STREAM stream1 (
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

## Lookup Table

The memory source supports lookup table. Below is an example to create a lookup table against memory topic `topicName`. Notice that, `KEY` property is required as a lookup table which will be served as a primary key for the virtual table and accelerate the query.

```text
CREATE TABLE alertTable() WITH (DATASOURCE="topicName", TYPE="memory", KIND="lookup", KEY="id")
```

After creating a memory lookup table, it will start to accumulate the data from the memory topic indexed by the key field. It will keep running independently of rules. Each topic and key pair will have a single in-memory copy of the virtual table. All rules that refer to the same table or the memory tables with the same topic/key pair will share the same copy of data.

The memory lookup table can be used like a pipeline between multiple rules which is similar to the [rule pipeline](../../rule_pipeline.md) concept. It can store the history of any stream type in memory so that other streams can work with. By working together with [updatable memory sink](../../sinks/builtin/memory.md#updatable-sink), the table content can be updated.