# Rule Pipeline

We can form rule pipelines by importing results of prior rule into the following rule. This is possible by employing intermediate storage or MQ such as mqtt broker. By using the pair of [memory source](../sources/builtin/memory.md) and [sink](../sinks/builtin/memory.md), we can create rule pipelines without external dependencies.

## Usage

Rule pipeline will be implicit. Each rule can use an memory sink / source. This means that each step will be created separately using existing api (example below).

```shell
#1 Create the source stream
{"sql" : "create stream demo () WITH (DATASOURCE=\"demo\", FORMAT=\"JSON\")"}

#2 Create rule and sink to memory
{
  "id": "rule1",
  "sql": "SELECT * FROM demo WHERE isNull(temperature)=false",
  "actions": [{
    "log": {
    },
    "memory": {
      "topic": "home/ch1/sensor1"
    }
  }]
}

#3 Create a stream from the memory topic
{"sql" : "create stream sensor1 () WITH (DATASOURCE=\"home/+/sensor1\", FORMAT=\"JSON\", TYPE=\"memory\")"}

#4 Create another rules to consume from the memory topic
{
  "id": "rule2-1",
  "sql": "SELECT avg(temperature) FROM sensor1 GROUP BY CountWindow(10)",
  "actions": [{
    "log": {
    },
    "memory": {
      "topic": "analytic/sensors"
    }
  }]
}

{
  "id": "rule2-2",
  "sql": "SELECT temperature + 273.15 as k FROM sensor1",
  "actions": [{
    "log": {
    }
  }]
}

```

By using the memory topic as the bridge, we now form a rule pipeline:
`rule1->{rule2-1, rule2-2}`. The pipeline can be multiple to multiple and very flexible. 

Notice that, the memory sink can be used together with other sinks to create multiple rule actions for a rule. And the memory source topic can use wildcard to subscirbe to a filtered topic list.

     