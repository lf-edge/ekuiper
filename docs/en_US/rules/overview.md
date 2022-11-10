# Rules

Rules are defined by JSON, below is an example.

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

The parameters for the rules are:

| Parameter name | Optional                         | Description                                                                  |
|----------------|----------------------------------|------------------------------------------------------------------------------|
| id             | false                            | The id of the rule. The rule id must be unique in the same eKuiper instance. |
| name           | true                             | The display name or description of a rule                                    |
| sql            | required if graph is not defined | The sql query to run for the rule                                            |
| actions        | required if graph is not defined | An array of sink actions                                                     |
| graph          | required if sql is not defined   | The json presentation of the rule's DAG(directed acyclic graph)              |
| options        | true                             | A map of options                                                             |
## Rule Logic

There are two ways to define the business logic of a rule. Either using SQL/actions combination or using the newly added graph API.

### SQL rule

By specifying the `sql` and `actions` property, we can define the business logic of a rule in a declarative way. Among these, `sql` defines the SQL query to run against a predefined stream which will transform the data. The output data can then route to multiple locations by `actions`. See [SQL](../sqls/overview.md) for more info of eKuiper SQL. 

#### Sources

eKuiper provides the following built-in sources,
  - MQTT source, see  [MQTT source stream](./sources/builtin/mqtt.md) for more detailed info.
  - EdgeX source by default is shipped in [docker images](https://hub.docker.com/r/lfedge/ekuiper), but NOT included in single download binary files, you use `make pkg_with_edgex` command to build a binary package that supports EdgeX source. Please see [EdgeX source stream](./sources/builtin/edgex.md) for more detailed info.
  - HTTP pull source, regularly pull the contents at user's specified interval time, see [here](./sources/builtin/http_pull.md) for more detailed info.
- Sources can be customized, see [extension](../extension/overview.md) for more detailed info.

#### Sinks/Actions

Currently, below kinds of sinks/actions are supported:

- [log](./sinks/builtin/log.md): Send the result to log file.
- [mqtt](./sinks/builtin/mqtt.md): Send the result to an MQTT broker.
- [edgex](./sinks/builtin/edgex.md): Send the result to EdgeX message bus.
- [rest](./sinks/builtin/rest.md): Send the result to a Rest HTTP server.
- [nop](./sinks/builtin/nop.md): Send the result to a nop operation.

Each action can define its own properties. There are several common properties:

| property name        | Type & Default Value               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|----------------------|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| concurrency          | int: 1                             | Specify how many instances of the sink will be run. If the value is bigger than 1, the order of the messages may not be retained.                                                                                                                                                                                                                                                                                                                                           |
| bufferLength         | int: 1024                          | Specify how many messages can be buffered in memory. If the buffered messages exceed the limit, the sink will block message receiving until the buffered messages have been sent out so that the buffered size is less than the limit.                                                                                                                                                                                                                                      |
| runAsync             | bool:false                         | Whether the sink will run asynchronously for better performance. If it is true, the sink result order is not promised.                                                                                                                                                                                                                                                                                                                                                      |
| omitIfEmpty          | bool: false                        | If the configuration item is set to true, when SELECT result is empty, then the result will not feed to sink operator.                                                                                                                                                                                                                                                                                                                                                      |
| sendSingle           | bool: false                        | The output messages are received as an array. This is indicate whether to send the results one by one. If false, the output message will be `{"result":"${the string of received message}"}`. For example, `{"result":"[{\"count\":30},"\"count\":20}]"}`. Otherwise, the result message will be sent one by one with the actual field name. For the same example as above, it will send `{"count":30}`, then send `{"count":20}` to the RESTful endpoint.Default to false. |
| dataTemplate         | string: ""                         | The [golang template](https://golang.org/pkg/html/template) format string to specify the output data format. The input of the template is the sink message which is always an array of map. If no data template is specified, the raw input will be the data.                                                                                                                                                                                                               |
| format               | string: "json"                     | The encode format, could be "json" or "protobuf". For "protobuf" format, "schemaId" is required and the referred schema must be registered.                                                                                                                                                                                                                                                                                                                                 |
| schemaId             | string: ""                         | The schema to be used to encode the result.                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| enableCache          | bool: default to global definition | whether to enable sink cache. cache storage configuration follows the configuration of the metadata store defined in `etc/kuiper.yaml`                                                                                                                                                                                                                                                                                                                                      |
| memoryCacheThreshold | int: default to global definition  | the number of messages to be cached in memory. For performance reasons, the earliest cached messages are stored in memory so that they can be resent immediately upon failure recovery. Data here can be lost due to failures such as power outages.                                                                                                                                                                                                                        |
| maxDiskCache         | int: default to global definition  | The maximum number of messages to be cached on disk. The disk cache is first-in, first-out. If the disk cache is full, the earliest page of information will be loaded into the memory cache, replacing the old memory cache.                                                                                                                                                                                                                                               |
| bufferPageSize       | int: default to global definition  | buffer pages are units of bulk reads/writes to disk to prevent frequent IO. if the pages are not full and eKuiper crashes due to hardware or software errors, the last unwritten pages to disk will be lost.                                                                                                                                                                                                                                                                |
| resendInterval       | int: default to global definition  | The time interval to resend information after failure recovery to prevent message storms.                                                                                                                                                                                                                                                                                                                                                                                   |
| cleanCacheAtStop     | bool: default to global definition | whether to clean all caches when the rule is stopped, to prevent mass resending of expired messages when the rule is restarted. If not set to true, the in-memory cache will be stored to disk once the rule is stopped. Otherwise, the memory and disk rules will be cleared out.                                                                                                                                                                                          |


##### Data Template

User can refer to [Use Golang template to customize analaysis result in eKuiper](./data_template.md) for more detailed scenarios.
If sendSingle is true, the data template will execute against a record; Otherwise, it will execute against the whole array of records. Typical data templates are:

For example, we have the sink input as

```
[]map[string]interface{}{{
    "ab" : "hello1",
},{
    "ab" : "hello2",
}}
```

In sendSingle=true mode:

- Print out the whole record

```
"dataTemplate": "{\"content\":{{json .}}}",
```

- Print out the ab field

```
"dataTemplate": "{\"content\":{{.ab}}}",
```

if the ab field is a string, add the quotes
```
"dataTemplate": "{\"content\":\"{{.ab}}\"}",
```

In sendSingle=false mode:

- Print out the whole record array

```
"dataTemplate": "{\"content\":{{json .}}}",
```

- Print out the first record

```
"dataTemplate": "{\"content\":{{json (index . 0)}}}",
```

- Print out the field ab of the first record

```
"dataTemplate": "{\"content\":{{index . 0 \"ab\"}}}",
```

- Print out field ab of each record in the array to html format

```
"dataTemplate": "<div>results</div><ul>{{range .}}<li>{{.ab}}</li>{{end}}</ul>",
```

Actions could be customized to support different kinds of outputs, see [extension](../extension/overview.md) for more detailed info.

###### Functions supported in template

With the help of template functions, users can do a lot of transformation including formation, simple mathematics, encoding etc. The supported functions in eKuiper template includes:

1. Go built-in [template functions](https://golang.org/pkg/text/template/#hdr-Functions).
2. An abundant extended function set from [sprig library](http://masterminds.github.io/sprig/).
3. eKuiper extended functions.

eKuiper extends several functions that can be used in data template.

- (deprecated)`json para1`: The `json` function is used for convert the map content to a JSON string. Use`toJson` from sprig instead.
- (deprecated)`base64 para1`: The `base64` function is used for encoding parameter value to a base64 string. Convert the pramater to string type and use `b64enc` from sprig instead.

##### Dynamic properties

In the sink, it is common to fetch a property value from the result data to achieve dynamic output. For example, to write data into a dynamic topic of mqtt. The dynamic properties will be parsed as a [data template](#data-template). In below example, the sink topic is gotten from the selected topic using data template.

```json
{
  "id": "rule1",
  "sql": "SELECT topic FROM demo",
  "actions": [{
    "mqtt": {
      "sendSingle": true,
      "topic": "prefix/{{.topic}}"
    }
  }]
}
```

In the above example, `sendSingle` property is used, so the sink data is a map by default. If not using `sendSingle`, you can get the topic by index with data template <code v-pre>{{index . 0 "topic"}}</code>.

#### Codecs

When the source of the rule reads in the event, it needs to parse and decode the various types of data from different types of sources into map type data for internal processing. After the rules are computed, the sink needs to encode the internal map type data into a proprietary format for various types of external systems. There are two main types of source/sink we support, one is that the connected external system has a fixed private format, so the codec work is already included in the source/sink implementation, e.g. EdgeX, Neuron; the other is that the connected external system only specifies the protocol of the connection, and the transmitted data allows a custom format, e.g. MQTT and ZeroMQ. The latter type of sourcing/sink must specify `format` and `schemaId` parameters to implement flexible codecs. For the management of supported codec formats and schemas, please refer to [codecs](./codecs.md).

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

The `graph` property is a json structure with `nodes` to define the nodes presented in the graph and `topo` to define the edge between nodes. The node type can be built-in node types such as window node and filter node etc. It can also be a user defined node from plugins. Please refer to [graph rule](./graph_rule.md) for more detail.

## Options

The current options includes:

| Option name        | Type & Default Value | Description                                                                                                                                                                                                                                                                                                                                       |
|--------------------|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| isEventTime        | boolean: false       | Whether to use event time or processing time as the timestamp for an event. If event time is used, the timestamp will be extracted from the payload. The timestamp filed must be specified by the [stream](../sqls/streams.md) definition.                                                                                                        |
| lateTolerance      | int64:0              | When working with event-time windowing, it can happen that elements arrive late. LateTolerance can specify by how much time(unit is millisecond) elements can be late before they are dropped. By default, the value is 0 which means late elements are dropped.                                                                                  |
| concurrency        | int: 1               | A rule is processed by several phases of plans according to the sql statement. This option will specify how many instances will be run for each plan. If the value is bigger than 1, the order of the messages may not be retained.                                                                                                               |
| bufferLength       | int: 1024            | Specify how many messages can be buffered in memory for each plan. If the buffered messages exceed the limit, the plan will block message receiving until the buffered messages have been sent out so that the buffered size is less than the limit. A bigger value will accommodate more throughput but will also take up more memory footprint. |
| sendMetaToSink     | bool:false           | Specify whether the meta data of an event will be sent to the sink. If true, the sink can get te meta data information.                                                                                                                                                                                                                           |
| sendError          | bool: true           | Whether to send the error to sink. If true, any runtime error will be sent through the whole rule into sinks. Otherwise, the error will only be printed out in the log.                                                                                                                                                                           |
| qos                | int:0                | Specify the qos of the stream. The options are 0: At most once; 1: At least once and 2: Exactly once. If qos is bigger than 0, the checkpoint mechanism will be activated to save states periodically so that the rule can be resumed from errors.                                                                                                |
| checkpointInterval | int:300000           | Specify the time interval in milliseconds to trigger a checkpoint. This is only effective when qos is bigger than 0.                                                                                                                                                                                                                              |
| restartStrategy    | struct               | Specify the strategy to automatic restarting rule after failures. This can help to get over recoverable failures without manual operations. Please check [Rule Restart Strategy](#rule-restart-strategy) for detail configuration items.                                                                                                          |

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