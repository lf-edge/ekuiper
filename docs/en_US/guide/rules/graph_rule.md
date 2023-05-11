# Graph Rule

Originally, eKuiper leverage SQL to define the rule logic. Although it is handy for developers, it is still not easy to use for users with no development knowledge. During runtime, rules are a DAG of elements(source/operator/sink) even when defining by SQL. The graph can be easily mapping to a drag and drop UI to facilitate the users. Thus, an alternative `graph` property is provided in the rule API.

The `graph` property is a JSON presentation of the DAG. It is consisted by `nodes` and `topo` which defines the nodes in the graph and their edges respectively. Below is a simplest rule defined by graph. It defines 3 nodes `demo`, `humidityFilter` and `mqttOut`. And the graph is linear as `demo` -> `humidityFilter` -> `mqttOut`. The rule will read from mqtt(`demo`), filter by humidity(`humidityFilter`) and sink to mqtt(`mqttOut`).

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
        "humidityFilter": ["mqttout"]
      }
    }
  }
}
```

## Nodes

Each node in the graph JSON has at least 3 fields:

- type: the type of the node, could be `source`, `operator` and `sink`.
- nodeType: the node type which defines the business logic of a node. There are various node types including built-in types and extended types defined by the plugins. 
- props: the properties for the node. It is different for each nodeType.

### Node Type

For source node, the nodeType is the type of the source like `mqtt` and `edgex`. Please refer to [source](../sources/overview.md) for all supported types. Notice that, all source node shared the same properties which is the same as the properties when [defining a stream](../../sqls/streams.md). The specific configuration are referred by `CONF_KEY`. In the below example, the nodeType specifies the source node is a mqtt source. The datasource and format property has the same meaning as defining a stream.

```json
  {
    "type": "source",
    "nodeType": "mqtt",
    "props": {
      "datasource": "devices/+/messages",
      "format":"json"
    }
  }
```

For sink node, the nodeType is the type of the sink like `mqtt` and `edgex`. Please refer to [sink](../sinks/overview.md) for all supported types. For all sink nodes, they share some common properties but each type will have some owned properties.


For operator node, the nodeType are newly defined. Each nodeType will have different properties.

### Source Node

The source node is the data source of the rule. It can be a stream or table. **User needs to define the stream/table before using it in the rule**. The `sourceType` property defines the type of the source. It can be `stream` or `table`. The `sourceName` property defines the name of the stream/table. The below example defines a source node which reads from a stream named `demoStream`. Please make sure the nodeType is the same as the type of the stream/table.

```json
  {
      "type": "source",
      "nodeType": "mqtt",
      "props": {
        "sourceType": "stream",
        "sourceName": "demoStream"
      }
  }
```

Currently, users can define the source node to refer to table as well. But only lookup table can be connected to Join node, scan table is not supported. The below example defines a source node which reads from a lookup table named `demoTable`. Please make sure the nodeType is the same as the type of the stream/table.

```json
  {
      "type": "source",
      "nodeType": "redis",
      "props": {
        "sourceType": "table",
        "sourceName": "demoTable"
      }
  }
```

### Built-in Operator Node Types

Currently, we supported the below node types for operator type.

#### function

This node defines a function call expression. The node return a new field with the name of the function or the alias name define in the expr property. It has only one property:

- expr: string, the function call expression.

Example:

```json
  {
    "type": "operator",
    "nodeType": "function",
    "props": {
      "expr": "log(temperature) as log_temperature"
    }
  }
```

#### aggfunc

This node defines an aggregate function call expression. The input for the node must be a collection of rows as the output of a window. The node will aggregate multiple rows into one aggregated row. For example, calculate the count of the window of 10 rows will produce only one row with field `count` = 10. Calculate the count of the grouped rows will produce one row for each group. It has only one property:

- expr: string, the aggregate function call expression.

Example:

```json
  {
    "type": "operator",
    "nodeType": "aggfunc",
    "props": {
      "expr": "count(*)"
    }
  }
```

#### filter

This node filter the data stream with a condition expression. It has only one propety:

- expr: string, the condition bool expression

Example:

```json
  {
    "type": "operator",
    "nodeType": "filter",
    "props": {
      "expr": "temperature > 20"
    }
  }
```

#### pick

This node selects the fields to be presented in the following workflow. It is usually used in the end of a workflow to define the data to be selected. It has only one property:

- fields: []string, the fields to be selected

Example:

```json
  {
    "type": "operator",
    "nodeType": "pick",
    "props": {
      "fields": ["log_temperature", "humidity", "window_end()"]
    }
  }
```

#### window

This node defines a [window](../../sqls/windows.md) in the workflow. It can accept multiple inputs but each input must be a single row. It will produce a collection of rows.

- type: string, the window type, available values are "tumblingwindow", "hoppingwindow", "slidingwindow", "sessionwindow" and "countwindow".
- unit: the time unit to be used. Check [time units](../../sqls/windows.md#time-units) for all available values.
- size: int, the window length.
- interval: int, the window trigger interval.

Example:

```json
  {
    "type": "operator",
    "nodeType": "window",
    "props": {
      "type": "hoppingwindow",
      "unit": "ss",
      "size": 10,
      "interval": 5
    }
  }
```

#### join

This node can merge data from different sources like a SQL join operation. The input must be a collection of row produced by a window. The output is another row collection whose rows are joined tuples. The properties are:

- from: string, the left source node to join.
- joins: an array of join conditions. Each join has the properties:
  - name: string, the right source node to join
  - type: string, the join type, could be inner, left, right, full, cross etc.
  - on: string, the bool expression to define the join condition

Example:

```json
   {
    "type": "operator",
    "nodeType": "join",
    "props": {
      "from": "device1",
      "joins": [
        {
          "name": "device2",
          "type": "inner",
          "on": "abs(device1.ts - device2.ts) < 200"
        }
      ]
    }
  }
```

Join operator supports to connect stream/stream join and stream/lookup table join. Stream/scan table join is not supported. If using stream/stream join, the prior node must be a window node. If using stream/lookup table join, only one join condition is supported. Below is an example of stream/lookup table join.

```json
   {
    "type": "operator",
    "nodeType": "join",
    "props": {
      "from": "demoStream",
      "joins": [
        {
          "name": "demoTable",
          "type": "inner",
          "on": "deviceStream.id = demoTable.id"
        }
      ]
    }
  }
```

#### groupby

This node defines the dimension to group by. The input must be a collection of rows. The output is a collection of grouped tuples. The properties are:

- dimensions: []string, the expressions of dimensions

Example:

```json
  {
    "type": "operator",
    "nodeType": "groupby",
    "props": {
      "dimensions": ["device1.humidity"]
    }
  }
```

#### orderby

This node will sort the input collection. So the input must be a collection of rows and the output will be the same type. The properties are:

- sorts: an array of sort conditions. Each condition has the properties:
  - field: string, the field to be sorted with.
  - order: string, the sorted direction, could be asc or desc.

Example:

```json
  {
    "type": "operator",
    "nodeType": "orderby",
    "props": {
      "sorts": [{
        "field": "count",
        "order": "desc"
      }]
    }
  }
```

#### switch

This node allows message to be routed to different branches of flows which is similar to switch statement in programming languages. Currently, this is the only node which have multiple output paths.

The switch node accepts multiple conditional expression as cases in order and evaluate events against the cases. The properties are:

- cases: the condition expressions to be evaluated in order.
- stopAtFirstMatch: whether to stop evaluate conditions when matching any condition, similarly to break in programming language.

In the edges definition, the output of the node has multiple paths, which is represented as a two-dimensional array. In
the following example, the switch node has two conditions defined in its `cases` property. Correspondingly, in edges ->
switch, you need to define a two-dimensional array of length 2 to specify the paths after the corresponding conditions
are met.

```json
{
  "id": "ruleSwitch",
  "name": "Demonstrate how to use switch node",
  "graph": {
    "nodes": {
      "abc": {
        "type": "source",
        "nodeType": "mqtt",
        "props": {
          "datasource": "demo",
          "confKey": "syno"
        }
      },
      "switch": {
        "type": "operator",
        "nodeType": "switch",
        "props": {
          "cases": [
            "temperature > 20",
            "temperature <= 20"
          ],
          "stopAtFirstMatch": true
        }
      },
      "mqttpv": {
        "type": "sink",
        "nodeType": "mqtt",
        "props": {
          "server": "tcp://syno.home:1883",
          "topic": "result/switch1",
          "sendSingle": true
        }
      },
      "mqttpv2": {
        "type": "sink",
        "nodeType": "mqtt",
        "props": {
          "server": "tcp://syno.home:1883",
          "topic": "result/switch2",
          "sendSingle": true
        }
      }
    },
    "topo": {
      "sources": [
        "abc"
      ],
      "edges": {
        "abc": [
          "switch"
        ],
        "switch": [
          [
            "mqttpv"
          ],
          [
            "mqttpv2"
          ]
        ]
      }
    }
  }
}
```

#### script

This node allows JavaScript code to be run against the messages that are passed through it.

- script: The inline javascript code to be run. 
- isAgg: Whether the node is for aggregated data.

There must be a function named `exec` defined in the script. If isAgg is false, the script node can accept a single message and must return a processed message. If isAgg is true, it will receive a message array (connected to window etc.) and must return an array.

1. Example to deal with single message.
   ```json
   {
     "type": "operator",
      "nodeType": "script",
      "props": {
        "script": "function exec(msg, meta) {msg.temperature = 1.8 * msg.temperature + 32; return msg;}"
      }
   }
   ```
2. Example to deal with window aggregated messages.
   ```json
   {
      "type": "operator",
      "nodeType": "script",
      "props": {
        "script": "function exec(msgs) {agg = {value:0}\nfor (let i = 0; i < msgs.length; i++) {\nagg.value = agg.value + msgs[i].value;\n}\nreturn agg;\n}",
        "isAgg": true
      }
   }
   ```