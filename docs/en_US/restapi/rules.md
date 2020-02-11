# Rules management

The Kuiper REST api for rules allows you to manage rules, such as create, show, drop, describe, start, stop and restart rules. 

## create a rule

The API accepts a JSON content and create and start a rule.
```shell
POST http://localhost:9081/rules
```
Request Sample

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [{
    "log":  {}
  }]
}
```


## show rules

The API is used for displaying all of rules defined in the server.

```shell
GET http://localhost:9081/rules
```

Response Sample:

```json
["rule1","rule2"]
```

## describe a rule

The API is used for print the detailed definition of rule.

```shell
GET http://localhost:9081/rules/{id}
```

Path parameter `id` is the id or name of the rule.

Response Sample: 

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

## drop a rule

The API is used for drop the rule.

```shell
DELETE http://localhost:8080/rules/{id}
```


## start a rule

The API is used to start running the rule.

```shell
POST http://localhost:8080/rules/{id}/start
```


## stop a rule

The API is used to stop running the rule.

```shell
POST http://localhost:8080/rules/{id}/stop
```

## restart a rule

The API is used to restart the rule.

```shell
POST http://localhost:8080/rules/{id}/restart
```

## get the status of a rule

The command is used to get the status of the rule. If the rule is running, the metrics will be retrieved realtime. The status can be
- running with metrics: $metrics
- stopped: $reason

```shell
GET http://localhost:8080/rules/{id}/status
```

Response Sample:

```shell
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