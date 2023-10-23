# Memory Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>
<span style="background:green;color:white;padding:1px;margin:2px">lookup table source</span>

The Memory source connector enables eKuiper to retrieve data from in-memory sources, primarily the [memory sink](../../sinks/builtin/memory.md). This connector plays an essential role in scenarios that require swift data retrieval without the overhead of disk or external service reads.

There's no need for additional configurations when integrating the Memory Source Connector with an eKuiper rule. Moreover, this connector is versatile, performing roles like a stream source, scan table source, or lookup table source.

## Create a Stream Source

As a [stream source](../../streams/overview.md), the connector continuously fetches data from a designated in-memory topic, making it ideal for real-time data processing.

Example:

```sql
CREATE STREAM stream1 (
    name STRING,
    size BIGINT,
    id BIGINT
) WITH (DATASOURCE="devices/result", FORMAT="json", TYPE="memory");
```

In this example, a memory stream source is defined to continuously pull data from the `devices/result` in-memory topic.

## Create a Scan Table Source

For querying or analyzing more static datasets, the Memory Source Connector can act as a [scan table source](../../tables/scan.md).

Example:

```sql
CREATE TABLE memoryTableDemo () WITH (DATASOURCE="topicB", FORMAT="JSON", TYPE="memory");
```

In this example, table `memoryTableDemo` allows for querying JSON-formatted data from the `topicB` in-memory topic.

## Create a Lookup Table Source

This mode allows the Memory Source Connector to serve as a lookup table source, enhancing data enrichment during stream processing.

Example：

```sql
CREATE TABLE memoryLookupTableDemo () WITH (DATASOURCE="topicC", FORMAT="JSON", TYPE="memory");
```

Besides specifying a `DATASOURCE`, which corresponds to a memory topic, you also need to specify the `KEY` property, which serves as the primary key for the virtual table, ensuring efficient data access.

Once set up, the memory lookup table will begin accumulating data from the specified memory topic. This data is indexed by the key field, allowing for rapid retrieval.

### **Key Features**

- **Independence**: The memory lookup table operates independently of any rules. This means that even if rules are modified or deleted, the data within the memory lookup table remains unaffected.
- **Data Sharing**: If multiple rules reference the same table or if there are multiple memory tables with identical topic/key pairs, they all share the same data set. This ensures consistency across different rules and streamlines data access.
- **Integration with Memory Sink**: The memory lookup table can be updated by integrating with an [updatable memory sink](../../sinks/builtin/memory.md#updatable-sink). This allows the table content to be refreshed as new data becomes available.
- **Rule Pipelining**: The memory lookup table can act as a bridge between multiple rules, akin to the rule pipeline concept. It enables one stream to store historical data in memory, which other streams can then access and utilize. This can be particularly useful for scenarios where historical data needs to be juxtaposed with real-time data for more informed decision-making.

## Topics in Memory Source

"Topic" in the Memory Source Connector signifies different in-memory data channels. Using the `DATASOURCE` property when defining a stream or table, users can pinpoint the memory topic they wish to access.

### Topic Wildcards

Similar to MQTT topics, wildcards are available:

- **+** : This is a single-level wildcard that replaces one topic level.
- **#** : This is a multi-level wildcard that can cover multiple topic levels. It's essential to note that this wildcard can only be used at the end of a topic.

**Examples**:

1. Subscribing to `home/device1/+/sensor1` would mean you're interested in messages from any device's `sensor1` located directly under `home/device1/`.
2. Subscribing to `home/device1/#` would mean you're interested in messages from `device1` and any of its sub-devices or sensors under the `home` directory.

## Rule Pipeline with Memory Source

The Memory Source Connector can be instrumental in constructing [rule pipelines](../../rules/rule_pipeline.md). These pipelines enable multiple rules to be chained, where one rule's output can be another's input. The internal format ensures data transfer efficiency, eliminating encoding or decoding needs. It's noteworthy that in this scenario, the `format` attribute of the memory source is ignored, ensuring optimal performance.
