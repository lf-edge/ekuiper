# Rules management

The Kuiper rule command line tools allows you to manage rules, such as create, show, drop, describe, start, stop and restart rules. 

## create a rule

The command is used for creating a rule.  The rule's definition is specified with JSON format, read [rule](../rules/overview.md) for more detailed information.

```shell
create rule $rule_name $rule_json | create rule $rule_name -f $rule_def_file
```

The rule can be created with two ways. 

- Specify the rule definition in command line.

Sample:

```shell
# bin/cli create rule rule1 {"sql": "SELECT * from demo","actions": [{"log":  {}},{"mqtt":  {"server":"tcp://127.0.0.1:1883", "topic":"demoSink"}}]}
```

The command create a rule named ``rule1``. 

- Specify the rule definition in file. If the rule is complex, or the rule is already wrote in text files with well organized formats, you can just specify the rule definition through ``-f`` option.

Sample:

```shell
# bin/cli create rule rule1 -f /tmp/rule.txt
```

Below is the contents of ``rule.txt``.

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

The command is used for displaying all of rules defined in the server.

```shell
show rules
```

Sample:

```shell
# bin/cli show rules
rule1
rule2
```

## describe a rule

The command is used for print the detailed definition of rule.

```shell
describe rule $rule_name
```

Sample: 

```shell
# bin/cli describe rule rule1
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
# bin/cli drop rule rule1
rule rule1 dropped
```

## start a rule

The command is used to start running the rule.

```shell
start rule $rule_name
```

Sample:

```shell
# bin/cli start rule rule1
rule rule1 started
```

## stop a rule

The command is used to stop running the rule.

```shell
stop rule $rule_name
```

Sample:

```shell
# bin/cli stop rule rule1
rule rule1 stopped
```

## restart a rule

The command is used to restart the rule.

```shell
restart rule $rule_name
```

Sample:

```shell
# bin/cli restart rule rule1
rule rule1 restarted
```

## get the status of a rule

The command is used to get the status of the rule. If the rule is running, the metrics will be retrieved realtime. The status can be
- running with metrics: $metrics
- stopped: $reason

```shell
getstatus rule $rule_name
```

Sample:

```shell
# bin/cli getstatus rule rule1
running with metrics:
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