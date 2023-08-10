# Rules management

The eKuiper rule command line tools allows you to manage rules, such as create, show, drop, describe, start, stop and restart rules.

## create a rule

The command is used for creating a rule.  The rule's definition is specified with JSON format, read [rule](../../guide/rules/overview.md) for more detailed information.

```shell
create rule $rule_name '$rule_json' | create rule $rule_name -f $rule_def_file
```

The rule can be created with two ways.

- Specify the rule definition in command line. Notice that, the json string must be quoted.

Sample:

```shell
# bin/kuiper create rule rule1 '{"sql": "SELECT * from demo","actions": [{"log":  {}},{"mqtt":  {"server":"tcp://127.0.0.1:1883", "topic":"demoSink"}}]}'
```

The command create a rule named `rule1`.

- Specify the rule definition in file. If the rule is complex, or the rule is already wrote in text files with well organized formats, you can just specify the rule definition through `-f` option.

Sample:

```shell
# bin/kuiper create rule rule1 -f /tmp/rule.txt
```

Below is the contents of `rule.txt`.

```json
{
  "sql": "SELECT * from demo",
  "actions": [
    {
      "log": {}
    },
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```

## show rules

The command is used for displaying all of rules defined in the server with a brief status.

```shell
show rules
```

Sample:

```shell
# bin/kuiper show rules
[
  {
    "id": "rule1",
    "status": "Running"
  },
  {
     "id": "rule2",
     "status": "Stopped: canceled by error."
  }
]
```

## describe a rule

The command is used for print the detailed definition of rule.

```shell
describe rule $rule_name
```

Sample:

```shell
# bin/kuiper describe rule rule1
{
  "sql": "SELECT * from demo",
  "actions": [
    {
      "log": {}
    },
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```

## drop a rule

The command is used for drop the rule.

```shell
drop rule $rule_name
```

Sample:

```shell
# bin/kuiper drop rule rule1
Rule rule1 is dropped.
```

## start a rule

The command is used to start running the rule.

```shell
start rule $rule_name
```

Sample:

```shell
# bin/kuiper start rule rule1
Rule rule1 was started.
```

## stop a rule

The command is used to stop running the rule.

```shell
stop rule $rule_name
```

Sample:

```shell
# bin/kuiper stop rule rule1
Rule rule1 was stopped.
```

## restart a rule

The command is used to restart the rule.

```shell
restart rule $rule_name
```

Sample:

```shell
# bin/kuiper restart rule rule1
Rule rule1 was restarted.
```

## get the status of a rule

The command is used to get the status of the rule. If the rule is running, the metrics will be retrieved realtime. The status can be

- $metrics
- stopped: $reason

```shell
getstatus rule $rule_name
```

Sample:

```shell
# bin/kuiper getstatus rule rule1
{
    "source_demo_0_records_in_total":5,
    "source_demo_0_records_out_total":5,
    "source_demo_0_exceptions_total":0,
    "source_demo_0_process_latency_ms":0,
    "source_demo_0_buffer_length":0,
    "source_demo_0_last_invocation":"2020-01-02T11:28:33.054821",
    ... 
    "op_filter_0_records_in_total":5,
    "op_filter_0_records_out_total":2,
    "op_filter_0_exceptions_total":0,
    "op_filter_0_process_latency_ms":0,
    "op_filter_0_buffer_length":0,
    "op_filter_0_last_invocation":"2020-01-02T11:28:33.054821",
    ...
}
```

## get the topology structure of a rule

The command is used to get the status of the rule represented as a json string. In the json string, there are 2 fields:

- sources: it is a string array of the names of all source nodes. They are the entry of the topology.
- edges: it is a hash map of all edges categorized by nodes. The keys are the starting point of an edge. And the value is a collection of ending point.

```shell
gettopo rule $rule_name
```

Sample result:

```json
{
  "sources": [
    "source_stream"
  ],
  "edges": {
    "op_project": [
      "sink_log"
    ],
    "source_stream": [
      "op_project"
    ]
  }
}
```

## validate a rule

The command is used for validating a rule.  The rule's definition is specified with JSON format, read [rule](../../guide/rules/overview.md) for more detailed information.

```shell
validate rule $rule_name '$rule_json' | validate rule $rule_name -f $rule_def_file
```

There are two ways to validate rules, which are the same as the two ways to create rules.

- Specify the rule definition in command line.

示例：

```shell
# bin/kuiper validate rule rule1 '{"sql": "SELECT * from demo","actions": [{"log":  {}},{"mqtt":  {"server":"tcp://127.0.0.1:1883", "topic":"demoSink"}}]}'
The rule has been successfully validated and is confirmed to be correct.
```

The command validate a rule named `rule1`.

- Specify the rule definition in file.

Sample:

```shell
# bin/kuiper validate rule rule1 -f /tmp/rule.txt
The rule has been successfully validated and is confirmed to be correct.
```

Below is the contents of `rule.txt`.

```json
{
  "sql": "SELECT * from demo",
  "actions": [
    {
      "log": {}
    },
    {
      "mqtt": {
        "server": "tcp://127.0.0.1:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```
