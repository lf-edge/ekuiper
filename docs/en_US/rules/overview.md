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

## id

The identification of the rule. The rule name cannot be duplicated in the same Kuiper instance.

## sql

The sql query to run for the rule. 

- Kuiper provides embeded following 2 sources,
  - MQTT source, see  [MQTT source stream](sources/mqtt.md) for more detailed info.
  - EdgeX source by default is shipped in [docker images](https://hub.docker.com/r/emqx/kuiper), but NOT included in single download binary files, you use ``make pkg_with_edgex`` command to build a binary package that supports EdgeX source. Please see [EdgeX source stream](sources/edgex.md) for more detailed info.
- See [SQL](../sqls/overview.md) for more info of Kuiper SQL.
- Sources can be customized, see [extension](../extension/overview.md) for more detailed info.

### sinks/actions

Currently, 3 kinds of sinks/actions are supported:

- [log](sinks/logs.md): Send the result to log file.
- [mqtt](sinks/mqtt.md): Send the result to an MQTT broker. 
- [edgex](sinks/edgex.md): Send the result to EdgeX message bus.
- [rest](sinks/rest.md): Send the result to a Rest HTTP server.
- [nop](sinks/nop.md): Send the result to a nop operation.

Each action can define its own properties. There are 3 common properties:

| property name | Type & Default Value | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| concurrency | int: 1   | Specify how many instances of the sink will be run. If the value is bigger than 1, the order of the messages may not be retained. |
| bufferLength | int: 1024   | Specify how many messages can be buffered in memory. If the buffered messages exceed the limit, the sink will block message receiving until the buffered messages have been sent out so that the buffered size is less than the limit. |
| runAsync        | bool:false   | Whether the sink will run asynchronously for better performance. If it is true, the sink result order is not promised.  |
| retryInterval   | int:1000   | Specify how many milliseconds will the sink retry to send data out if the previous send failed  |
| cacheLength     | int:10240   | Specify how many messages can be cached. The cached messages will be resent to external system until the data sent out successfully. The cached message will be sent in order except in runAsync or concurrent mode. The cached message will be saved to disk in fixed intervals.  |
| cacheSaveInterval  | int:1000   | Specify the interval to save cached message to the disk. Notice that, if the rule is closed in plan, all the cached messages will be saved at close. A larger value can reduce the saving overhead but may lose more cache messages when the system is interrupted in error.  |
| omitIfEmpty | bool: false | If the configuration item is set to true, when SELECT result is empty, then the result will not feed to sink operator. |

Actions could be customized to support different kinds of outputs, see [extension](../extension/overview.md) for more detailed info.

### options
The current options includes:

| Option name | Type & Default Value | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| isEventTime | boolean: false   | Whether to use event time or processing time as the timestamp for an event. If event time is used, the timestamp will be extracted from the payload. The timestamp filed must be specified by the [stream]([extension](../sqls/streams.md)) definition. |
| lateTolerance        | int64:0   | When working with event-time windowing, it can happen that elements arrive late. LateTolerance can specify by how much time(unit is millisecond) elements can be late before they are dropped. By default, the value is 0 which means late elements are dropped.  |
| concurrency | int: 1   | A rule is processed by several phases of plans according to the sql statement. This option will specify how many instances will be run for each plan. If the value is bigger than 1, the order of the messages may not be retained. |
| bufferLength | int: 1024   | Specify how many messages can be buffered in memory for each plan. If the buffered messages exceed the limit, the plan will block message receiving until the buffered messages have been sent out so that the buffered size is less than the limit. A bigger value will accommodate more throughput but will also take up more memory footprint.  |
