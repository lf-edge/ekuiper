# Rules management

The eKuiper REST api for rules allows you to manage rules, such as create, show, drop, describe, start, stop and restart rules.

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

The API is used for displaying all of rules defined in the server with a brief status.

```shell
GET http://localhost:9081/rules
```

Response Sample:

```json
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

## update a rule

The API accepts a JSON content and update a rule.

```shell
PUT http://localhost:9081/rules/{id}
```

Path parameter `id` is the id or name of the old rule.

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

## drop a rule

The API is used for drop the rule.

```shell
DELETE http://localhost:9081/rules/{id}
```

## start a rule

The API is used to start running the rule.

```shell
POST http://localhost:9081/rules/{id}/start
```

## stop a rule

The API is used to stop running the rule.

```shell
POST http://localhost:9081/rules/{id}/stop
```

## restart a rule

The API is used to restart the rule.

```shell
POST http://localhost:9081/rules/{id}/restart
```

## get the status of a rule

The command is used to get the status of the rule. If the rule is running, the metrics will be retrieved realtime. The status can be

- $metrics
- stopped: $reason

```shell
GET http://localhost:9081/rules/{id}/status
```

Response Sample:

```shell
{
    "lastStartTimestamp": 0,
    "lastStopTimestamp":0,
    "nextStartTimestamp":0,
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

Among them, the following states respectively represent the unix timestamp of the last start and stop of the rule. When the rule is a periodic rule, you can use `nextStartTimestamp` to view the unix timestamp of the next start of the rule.

```shell
{
    "lastStartTimestamp": 0,
    "lastStopTimestamp":0,
    "nextStartTimestamp":0,
    ...
}
```

## get the status of all rules

The command is used to get the status of all rules. If the rule is running, the metrics will be retrieved realtime.

```shell
GET http://localhost:9081/rules/status/all
```

## get the topology structure of a rule

The command is used to get the status of the rule represented as a json string. In the json string, there are 2 fields:

- sources: it is a string array of the names of all source nodes. They are the entry of the topology.
- edges: it is a hash map of all edges categorized by nodes. The keys are the starting point of an edge. And the value is a collection of ending point.

```shell
GET http://localhost:9081/rules/{id}/topo
```

Response Sample:

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

The API accepts a JSON content and validate a rule.

```shell
POST http://localhost:9081/rules/validate
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

For the API, here is the explanation of the status codes:
- If the request body is incorrect, a status code of 400 will be returned, indicating an invalid request.
- If the rule validation fails, a status code of 422 will be returned, indicating an invalid rule.
- If the rule validation passes, a status code of 200 will be returned, indicating a valid and successfully validated rule.

## Query Rule Plan

The API is used to get the plan of the SQL.

```shell
GET  http://localhost:9081/rules/{id}/explain
```
