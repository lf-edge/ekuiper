# Rules

eKuiper's stream processing capabilities are powered by its rules. Rules are the backbone of data flow, dictating how data is ingested, transformed, and then exported to external systems.

A rule is a JSON-defined stream processing flow. It bridges the gap between data sources and processing logic before leading to actions that send the data to external systems.

```json
{
  "id": "rule1",
  "sql": "SELECT demo.temperature, demo1.temp FROM demo left join demo1 on demo.timestamp = demo1.timestamp where demo.temperature > demo1.temp GROUP BY demo.temperature, HOPPINGWINDOW(ss, 20, 10)",
  "actions": [
    {
      "log": {}
    },
    {
      "mqtt": {
        "server": "tcp://47.52.67.87:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```

Key components of a rule:

- **ID**: A unique identifier.
- **SQL**: The processing logic, built on eKuiper's extended SQL syntax.
- **Actions**: List of sink actions dictating where the processed data is sent.

The table below is a detailed explanation of the row component:

| Parameter name | Optional                         | Description                                                                  |
|----------------|----------------------------------|------------------------------------------------------------------------------|
| id             | false                            | The id of the rule. The rule id must be unique in the same eKuiper instance. |
| name           | true                             | The display name or description of a rule                                    |
| sql            | required if graph is not defined | The sql query to run for the rule                                            |
| actions        | required if graph is not defined | An array of sink actions                                                     |
| graph          | required if sql is not defined   | The json presentation of the rule's DAG(directed acyclic graph)              |
| options        | true                             | A map of options                                                             |
| triggerd       | true                             | Whether to start the rule after creation. Default is true.                   |
| tags           | yes                              | string list, rule tags, used to filter rules                                 |

## Rule Logic

A rule represents a stream processing flow from a data source that ingest data into the flow to various processing logic to actions that engest the data to external systems.

There are two ways to define the flow aka. business logic of a rule. Either using SQL/actions combination or using the newly added graph API.

1. **SQL Query Approach**: Using a combination of SQL and actions for a more declarative approach.
2. **Graph API Approach**: Introduced in eKuiper 1.6.0, this method represents the rule as a Directed Acyclic Graph (DAG) in JSON, ideal for graphical user interfaces.

### SQL Query

By specifying the `sql` and `actions` property, we can define the business logic of a rule in a declarative way. Among these, `sql` defines the SQL query to run against a predefined stream which will transform the data. The output data can then be routed to multiple locations by `actions`.

#### SQL

The simplest rule SQL is like `SELECT * FROM demo`. It has ANSI SQL-like syntax and can leverage abundant operators and functions provided by eKuiper runtime. See [SQL](../../sqls/overview.md) for more information of eKuiper SQL.

Most of the SQL clauses define the logic except the `FROM` clause, which is responsible for specifying the stream. In this example, `demo` is the stream. It is possible to have multiple streams or streams/tables by using a join clause. As a streaming engine, there must be at least one stream in a rule.

Thus, the SQL query here actually defines two parts:

- The stream(s) or table(s) to be processed.
- How to process.

Before using the SQL rule, the stream must be defined in prior. Please check [streams](../streams/overview.md) for details.

#### Actions

The actions part defines the output action for a rule. Each rule can have multiple actions. An action is an instance of a sink connector. When define actions, the key is the sink connector type name, and the value is the properties.

eKuiper has built in abundant sink connector type such as mqtt, rest and file. Users can also extend more sink type to be used in a rule action. Each sink type have its own property set. For more detail, please check [sink](../sinks/overview.md).

### Graph rule

Since eKuiper 1.6.0, eKuiper provides graph property in the rule model as an alternative way to create a rule. The property defines the DAG of a rule in JSON format. It is easy to map it directly to a graph in a GUI editor and suitable to serve as the backend of a drag and drop UI. An example of the graph rule definition is as below:

```json
{
  "id": "rule1",
  "name": "Test Condition",
  "graph": {
    "nodes": {
      "demo": {
        "type": "source",
        "nodeType": "mqtt",
        "props": {
          "datasource": "devices/+/messages"
        }
      },
      "humidityFilter": {
        "type": "operator",
        "nodeType": "filter",
        "props": {
          "expr": "humidity > 30"
        }
      },
      "logfunc": {
        "type": "operator",
        "nodeType": "function",
        "props": {
          "expr": "log(temperature) as log_temperature"
        }
      },
      "tempFilter": {
        "type": "operator",
        "nodeType": "filter",
        "props": {
          "expr": "log_temperature < 1.6"
        }
      },
      "pick": {
        "type": "operator",
        "nodeType": "pick",
        "props": {
          "fields": ["log_temperature as temp", "humidity"]
        }
      },
      "mqttout": {
        "type": "sink",
        "nodeType": "mqtt",
        "props": {
          "server": "tcp://${mqtt_srv}:1883",
          "topic": "devices/result"
        }
      }
    },
    "topo": {
      "sources": ["demo"],
      "edges": {
        "demo": ["humidityFilter"],
        "humidityFilter": ["logfunc"],
        "logfunc": ["tempFilter"],
        "tempFilter": ["pick"],
        "pick": ["mqttout"]
      }
    }
  }
}
```

The `graph` property is a json structure with `nodes` to define the nodes presented in the graph and `topo` to define the edge between nodes. The node type can be built-in node types such as window node and filter node etc. It can also be a user-defined node from plugins. Please refer to [graph rule](./graph_rule.md) for more detail.

## Fine Tuning

eKuiper provides a slew of options to fine-tune rule behavior, including:

- **Debugging and Logging**: Control log verbosity and direct logs to specific files.
- **Event Time**: Choose between event time or processing time for timestamping.
- **Fault Tolerance**: Define behavior for late-arriving events.
- **Concurrency**: Manage parallel processing for different rule phases.
- **Buffering**: Control in-memory message buffering.
- **QoS and Checkpointing**: Ensure data reliability with Quality of Service levels and periodic state saving.
- **Restart Strategy**: Define how rules should restart after failures.
- **Scheduled Rules**: Set up periodic rule execution based on cron expressions.

See the table below for a detailed explanation of each rule behavior:

| Option name        | Type & Default Value | Description                                                                                                                                                                                                                                                                                                                                       |
|--------------------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| debug              | bool: false          | Specify whether to enable the debug level for this rule. By default, it will inherit the Debug configuration parameters in the global configuration.                                                                                                                                                                                              |
| logFilename        | string: ""           | Specify the name of a separate log file for this rule, and the log will be saved in the global log folder. By default, the log configuration parameters in the global configuration will be used.                                                                                                                                                 |
| isEventTime        | boolean: false       | Whether to use event time or processing time as the timestamp for an event. If event time is used, the timestamp will be extracted from the payload. The timestamp filed must be specified by the [stream](../../sqls/streams.md) definition.                                                                                                     |
| lateTolerance      | int64:0              | When working with event-time windowing, it can happen that elements arrive late. LateTolerance can specify by how much time(unit is millisecond) elements can be late before they are dropped. By default, the value is 0 which means late elements are dropped.                                                                                  |
| concurrency        | int: 1               | A rule is processed by several phases of plans according to the sql statement. This option will specify how many instances will be run for each plan. If the value is bigger than 1, the order of the messages may not be retained.                                                                                                               |
| bufferLength       | int: 1024            | Specify how many messages can be buffered in memory for each plan. If the buffered messages exceed the limit, the plan will block message receiving until the buffered messages have been sent out so that the buffered size is less than the limit. A bigger value will accommodate more throughput but will also take up more memory footprint. |
| sendMetaToSink     | bool:false           | Specify whether the meta data of an event will be sent to the sink. If true, the sink can get te meta data information.                                                                                                                                                                                                                           |
| sendError          | bool: false          | Whether to send the error to sink. If true, any runtime error will be sent through the whole rule into sinks. Otherwise, the error will only be printed out in the log.                                                                                                                                                                           |
| qos                | int:0                | Specify the qos of the stream. The options are 0: At most once; 1: At least once and 2: Exactly once. If qos is bigger than 0, the checkpoint mechanism will be activated to save states periodically so that the rule can be resumed from errors.                                                                                                |
| checkpointInterval | int:300000           | Specify the time interval in milliseconds to trigger a checkpoint. This is only effective when qos is bigger than 0.                                                                                                                                                                                                                              |
| restartStrategy    | struct               | Specify the strategy to automatic restarting rule after failures. This can help to get over recoverable failures without manual operations. Please check [Rule Restart Strategy](#rule-restart-strategy) for detail configuration items.                                                                                                          |
| cron               | string: ""           | Specify the periodic trigger strategy of the rule, which is described by [cron expression](https://en.wikipedia.org/wiki/Cron)                                                                                                                                                                                                                    |
| duration           | string: ""           | Specifies the running duration of the rule, only valid when cron is specified. The duration should not exceed the time interval between two cron cycles, otherwise it will cause unexpected behavior.                                                                                                                                             |
| cronDatetimeRange  | lists of struct      | Specify the effective time period of the Scheduled Rule, which is only valid when `cron` is specified. When this `cronDatetimeRange` is specified, the Scheduled Rule will only take effect within the time range specified. Please see [Scheduled Rule](#Scheduled Rule) for detailed configuration items                                        |
| enableRuleTracer   | bool: false          | Specify whether the rule enables rule-level data tracing                                                                                                                                                                                                                                                                                          |
| sendNilField       | bool: false          | Specify whether to output columns with a value of nil as specified by the rules.                                                                                                                                                                                                                                                                  |
| planOptimizeStrategy | struct | Specify whether the rule turns on the corresponding optimization |
| disableBufferFullDiscard | bool: false | Whether to enable the behavior of discarding data when the buffer is full                                                                           |

For detail about `qos` and `checkpointInterval`, please check [state and fault tolerance](./state_and_fault_tolerance.md).

The rule options can be defined globally in `etc/kuiper.yaml` under the `rules` section. The options defined in the rule json will override the global setting.

### Rule Restart Strategy

The restart strategy options include:

| Option name  | Type & Default Value | Description                                                                                                                           |
|--------------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------|
| attempts     | int: 0               | The maximum retry times. If set to 0, the rule will fail immediately without retrying.                                                |
| delay        | int: 1000            | The default interval in millisecond to retry. If `multiplier` is not set, the retry interval will be fixed to this value.             |
| maxDelay     | int: 30000           | The maximum interval in millisecond to retry. Only effective when `multiplier` is set so that the delay will increase for each retry. |
| multiplier   | float: 2             | The exponential to increase the interval.                                                                                             |
| jitterFactor | float: 0.1           | How large random value will be added or subtracted to the delay to prevent restarting multiple rules at the same time.                |

The default values can be changed by editing the `etc/kuiper.yaml` file.

### Scheduled Rule

Rules support periodic start, run and pause. In options, `cron` expresses the starting policy of the periodic rule, such as starting every 1 hour, and `duration` expresses the running time when the rule is started each time, such as running for 30 minutes.

When `cron` is every 1 hour and `duration` is 30 minutes, then the rule will be started every 1 hour, and will be suspended after 30 minutes each time, waiting for the next startup.

When a periodic rule is stopped by [stop rule](../../api/restapi/rules.md#stop-a-rule), the rule will be removed from the periodic scheduler and will no longer be scheduled to run. If the rule is running, it will also be paused.

`cronDatetimeRange`configuration items are like following:

| Option name | Type & Default Value | Description                                                                                       |
|-------------|----------------------|---------------------------------------------------------------------------------------------------|
| begin       | string               | The begin time of the effective period of the scheduled rule, the format is `YYYY-MM-DD hh:mm:ss' |
| end         | string               | The end time of the effective period of the scheduled rule, the format is `YYYY-MM-DD hh:mm:ss'   |
| beginTimestamp | int | The starting unix timestamp of the period in which the periodic rule takes effect, in ms |
| endTimestamp | int | The end unix timestamp of the period in which the periodic rule takes effect, in ms |

`cronDatetimeRange` supports lists of struct, you can declare a set of time ranges to express multiple time ranges for scheduled rules to take effect:

```json
{
    "cronDatetimeRange": [
        {
            "begin": "2023-06-26 10:00:00",
            "end": "2023-06-26 20:00:00"
        },
        {
            "beginTimestamp": 1701401478000,
            "endTimestamp": 1701401578000
        }
    ]
}
```

#### Phase run rules

When `cronDatetimeRange` is configured but `cron` and `duration` are empty, the rule will run according to the time period specified by `cronDatetimeRange` until the time period is exceeded.

### Rule optimization switch

The rule optimization switch `planOptimizeStrategy` can control whether the rule enables specific rule optimization:

The configuration items of `planOptimizeStrategy` are as follows:

| option name             | type and default value | description                                                                                                                              |
|-------------------------|------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| enableIncrementalWindow | bool: false            | Enable incremental calculation when the rule contains both a time window and an aggregate function that supports incremental calculation |

## View Rule Status

The rule startup process is asynchronous. When a user sends a start command, eKuiper performs necessary static checks
and then asynchronously executes the rule's startup operation. Therefore, the command response received by the user only
indicates that eKuiper has accepted the startup request and set the rule's Expected Status to 'started'. This does not
mean the rule has begun running. Furthermore, a rule that is already running may stop due to runtime exceptions. We can
get the running status of all rules and the detailed status of a single rule through the rest API.

The status of all rules can be obtained through [Show Rules](../../api/restapi/rules.md#show-rules), and the status of a single rule can be obtained through [getting the status of a rule](../../api/restapi/rules.md#get-the-status-of-a-rule).

### Understanding Status of Running Rules

For the following rules:

```json
{
  "id": "rule",
  "sql": "select * from demo",
  "actions": [
     {
      "mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "devices/+/messages",
        "qos": 1,
        "clientId": "demo_001",
        "retained": false
      }
    }
  ]
}
```

We can get the status from the above `get-the-status-of-a-rule`:

```json
{
  "status": "running",
  "source_demo_0_records_in_total": 0,
  "source_demo_0_records_out_total": 0,
  ......
  "op_2_project_0_records_in_total": 0,
  "op_2_project_0_records_out_total": 0,
  ......
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  ......
}
```

`status` represents the current running status of the rule, and `running` represents that the rule is running.

The monitoring items represent the operation status of each operator during the rule running process, and the monitoring items are composed of `operator_type information_operator concurrency_index actual_monitoring_items`.

Take `source_demo_0_records_in_total` as an example, where `source` represents the operator for reading data, `demo` is the corresponding stream, `0` represents the index of the operator instance in the concurrency, and `records_in_total` interprets the actual the monitoring item, that is, how many records the operator has received.

When we try to send a record to the stream, the status of the rule is obtained again as follows:

```json
{
  "status": "running",
  "source_demo_0_records_in_total": 1,
  "source_demo_0_records_out_total": 1,
  ......
  "op_2_project_0_records_in_total": 1,
  "op_2_project_0_records_out_total": 1,
  ......
  "sink_mqtt_0_0_records_in_total": 1,
  "sink_mqtt_0_0_records_out_total": 1,
  ......
}
```

It can be seen that `records_in_total` and `records_out_total` of each operator have changed from 0 to 1, which means that the operator has received a record and passed a record to the next operator, and finally sent to the `sink` and the `sink` wrote 1 record.

If Prometheus configuration is enabled, these metrics will also be collected by Prometheus. For a complete list of
operational metrics, please refer to the [Metrics List](../../operation/usage/monitor_with_prometheus.md#metric-types).

## Versioning

The rule can have an optional **version** field to control updates. When you update a rule, the system compares the new
version string to the existing one.
An update is only accepted if the new version is **lexically greater** than the old one. This comparison is a
character-by-character string comparison, not a numerical one. The control logic for all versioned APIs is the same;
please refer to the [Versioning Logic](#versioning-logic) for details.

### Versioning Logic

- **No Version Specified:** If neither the old nor the new schema has a `version` field, the update will proceed. This
  behavior aligns with the original, unversioned logic.
- **Versioning Set:** If a `version` field is present in either the old or the new schema, the system will always
  perform a version comparison. The presence of any version string triggers the new comparison logic.
- **Lexical Comparison:** Updates are based on a lexical (string) comparison. The new schema's `version` must be
  lexicographically greater than the current one for the update to be successful.
- **Smallest Version:** A schema without a `version` field is considered to have the "smallest possible" version. This
  means that adding a version field to an existing, unversioned schema will always result in a successful update, as any
  new version string will be lexically greater than the non-existent one.

To avoid confusion and ensure correct ordering, it's highly recommended to use a **timestamp** as the version string.
Timestamps, such as Unix epoch time, provide a universally unique and monotonically increasing value that naturally
satisfies the lexical comparison rule.
