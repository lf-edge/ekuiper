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

## get schema of a rule

This API allows you to retrieve the output schema of a specific rule. The schema describes the fields and their
properties (like hasIndex and index) that are produced by the rule's SELECT statement.

```shell
GET http://localhost:9081/rules/{id}
```

Path parameter `id` is the id of the rule.

Example response when using slice mode:

```json
{
  "id": {
    "hasIndex": true,
    "index": 0
  },
  "name": {
    "hasIndex": true,
    "index": 1
  }
}
```

## upsert a rule

The API accepts a JSON content and upsert a rule which means if the rule is not existed, create it; otherwise, update
it. If update fails, the original rule will continue running.

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

The API is used to start running the rule. Please note that the command only indicates the successful transmission of
the start instruction. To verify if the rule has completed startup, the rule status must be checked. If the rule is
currently in the process of starting or stopping, the start instruction will be added to the rule's command queue.

```shell
POST http://localhost:9081/rules/{id}/start
```

## stop a rule

The API is used to stop running the rule. Please note that the command only indicates the successful transmission of the
stop instruction. To verify if the rule has completed startup, the rule status must be checked. If the rule is currently
in the process of starting or stopping, the start instruction will be added to the rule's command queue.

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

## Get rule CPU information

```shell
GET http://localhost:9081/rules/usage/cpu

{
    "rule1": 220,
    "rule2": 270
}
```

Get the CPU time used by all rules in the past 30 seconds, in milliseconds.

## Reset Tags

This API is used to reset tags to rules

```shell
PUT /rules/{id}/tags

{
  "tags": ["t1","t2"]
}
```

## Add tags on rules

This API is used to add tags to rules

```shell
PATCH /rules/{id}/tags

{
  "tags": ["t1","t2"]
}
```

## Delete tags on rules

This API is used to delete tags from rules

```shell
DELETE /rules/{id}/tags

{
  "keys":["key1","key2"]
}
```

## Query rules based on tags

This API is used to query rules containing a given tags and return a list of rule names that meet the conditions

```shell
GET /rules/tags/match

{
  "keys":["key1","key2"]
}
```

## Bulk start / stop rules by tag

These APIs are used to start or stop multiple rules based on the assigned tags.

- bulk start rules

```shell
POST /rules/bulkstart

{
  "tags": ["t1"]
}
```

- bulk stop rules

```shell
POST /rules/bulkstop

{
  "tags": ["t1"]
}
```

Both APIs return a list of rules with the operation result for each rule, indicating whether the operation was successful or failed.
In case of failure, an error message is returned for the affected rule.

These APIs are not atomic. If an error occurs during execution, some rules may be started or stopped successfully while others may not.

