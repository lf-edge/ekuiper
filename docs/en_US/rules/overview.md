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

The following 3 parameters are required for creating a rule.

## Parameters

| Parameter name | Optional | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| id | false   | The id of the rule |
| sql        | false   | The sql query to run for the rule |
| actions           | false    | An array of sink actions        |
| options           | true    | A map of options        |

### id

The identification of the rule. The rule name cannot be duplicated in the same eKuiper instance.

### sql

The sql query to run for the rule.

## Options

The current options includes:

| Option name | Type & Default Value | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| isEventTime | boolean: false   | Whether to use event time or processing time as the timestamp for an event. If event time is used, the timestamp will be extracted from the payload. The timestamp filed must be specified by the [stream](../sqls/streams.md) definition. |
| lateTolerance        | int64:0   | When working with event-time windowing, it can happen that elements arrive late. LateTolerance can specify by how much time(unit is millisecond) elements can be late before they are dropped. By default, the value is 0 which means late elements are dropped.  |
| concurrency | int: 1   | A rule is processed by several phases of plans according to the sql statement. This option will specify how many instances will be run for each plan. If the value is bigger than 1, the order of the messages may not be retained. |
| bufferLength | int: 1024   | Specify how many messages can be buffered in memory for each plan. If the buffered messages exceed the limit, the plan will block message receiving until the buffered messages have been sent out so that the buffered size is less than the limit. A bigger value will accommodate more throughput but will also take up more memory footprint.  |
| sendMetaToSink | bool:false   | Specify whether the meta data of an event will be sent to the sink. If true, the sink can get te meta data information.  |
| sendError  | bool: true | Whether to send the error to sink. If true, any runtime error will be sent through the whole rule into sinks. Otherwise, the error will only be printed out in the log. |
| qos | int:0   | Specify the qos of the stream. The options are 0: At most once; 1: At least once and 2: Exactly once. If qos is bigger than 0, the checkpoint mechanism will be activated to save states periodically so that the rule can be resumed from errors.  |
| checkpointInterval | int:300000   | Specify the time interval in milliseconds to trigger a checkpoint. This is only effective when qos is bigger than 0.  |

For detail about `qos` and `checkpointInterval`, please check [state and fault tolerance](./state_and_fault_tolerance.md).

The rule options can be defined globally in ``etc/kuiper.yaml`` under the ``rules`` section. The options defined in the rule json will override the global setting.

## Sources

- eKuiper provides embeded following 3 sources,
  - MQTT source, see  [MQTT source stream](./sources/mqtt.md) for more detailed info.
  - EdgeX source by default is shipped in [docker images](https://hub.docker.com/r/lfedge/ekuiper), but NOT included in single download binary files, you use ``make pkg_with_edgex`` command to build a binary package that supports EdgeX source. Please see [EdgeX source stream](./sources/edgex.md) for more detailed info.
  - HTTP pull source, regularly pull the contents at user's specified interval time, see [here](./sources/http_pull.md) for more detailed info.
- See [SQL](../sqls/overview.md) for more info of eKuiper SQL.
- Sources can be customized, see [extension](../extension/overview.md) for more detailed info.

## Sinks/Actions

Currently, below kinds of sinks/actions are supported:

- [log](./sinks/logs.md): Send the result to log file.
- [mqtt](./sinks/mqtt.md): Send the result to an MQTT broker.
- [edgex](./sinks/edgex.md): Send the result to EdgeX message bus.
- [rest](./sinks/rest.md): Send the result to a Rest HTTP server.
- [nop](./sinks/nop.md): Send the result to a nop operation.

Each action can define its own properties. There are several common properties:

| property name | Type & Default Value | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| concurrency | int: 1   | Specify how many instances of the sink will be run. If the value is bigger than 1, the order of the messages may not be retained. |
| bufferLength | int: 1024   | Specify how many messages can be buffered in memory. If the buffered messages exceed the limit, the sink will block message receiving until the buffered messages have been sent out so that the buffered size is less than the limit. |
| runAsync        | bool:false   | Whether the sink will run asynchronously for better performance. If it is true, the sink result order is not promised.  |
| retryInterval   | int:1000   | Specify how many milliseconds will the sink retry to send data out if the previous send failed. If the specified value <= 0, then it will not retry. |
| retryCount | int:3 | Specify how many will the sink retry to send data out if the previous send failed. If the specified value <= 0, then it will not retry. |
| cacheLength     | int:1024   | Specify how many messages can be cached. The cached messages will be resent to external system until the data sent out successfully. The cached message will be sent in order except in runAsync or concurrent mode. The cached message will be saved to disk in fixed intervals.  |
| cacheSaveInterval  | int:1000   | Specify the interval to save cached message to the disk. Notice that, if the rule is closed in plan, all the cached messages will be saved at close. A larger value can reduce the saving overhead but may lose more cache messages when the system is interrupted in error.  |
| omitIfEmpty | bool: false | If the configuration item is set to true, when SELECT result is empty, then the result will not feed to sink operator. |
| sendSingle        | true     | The output messages are received as an array. This is indicate whether to send the results one by one. If false, the output message will be ``{"result":"${the string of received message}"}``. For example, ``{"result":"[{\"count\":30},"\"count\":20}]"}``. Otherwise, the result message will be sent one by one with the actual field name. For the same example as above, it will send ``{"count":30}``, then send ``{"count":20}`` to the RESTful endpoint.Default to false. |
| dataTemplate      | true     | The [golang template](https://golang.org/pkg/html/template) format string to specify the output data format. The input of the template is the sink message which is always an array of map. If no data template is specified, the raw input will be the data. |

### Data Template

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

### Functions supported in template

With the help of template functions, users can do a lot of transformation including formation, simple mathematics, encoding etc. The supported functions in eKuiper template includes:

1. Go built-in [template functions](https://golang.org/pkg/text/template/#hdr-Functions).
2. An abundant extended function set from [sprig library](http://masterminds.github.io/sprig/).
3. eKuiper extended functions.
   
eKuiper extends several functions that can be used in data template.

- (deprecated)`json para1`: The `json` function is used for convert the map content to a JSON string. Use`toJson` from sprig instead.
- (deprecated)`base64 para1`: The `base64` function is used for encoding parameter value to a base64 string. Convert the pramater to string type and use `b64enc` from sprig instead.

### Dynamic properties

In the sink, it is common to fetch a property value from the result data to achieve dynamic output. For example, to write data into a dynamic topic of mqtt. The dynamic properties will all follow the json path syntax. In below example, the sink topic is gotten from the selected topic by using jsonpath syntax.

```json
{
  "id": "rule1",
  "sql": "SELECT topic FROM demo",
  "actions": [{
    "mqtt": {
      "sendSingle": true,
      "topic": "$.topic"
    }
  }]
}
```

In the above example, `sendSingle` property is used, so the sink data is a map by default. If not using `sendSingle`, you can get the topic by index with jsonapth `$[0].topic`.