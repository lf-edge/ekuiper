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

The identification of the rule. The rule name cannot be duplicated in the same XStream instance.

## sql

The sql query to run for the rule. 

- XStream provides embeded support MQTT source, see  [MQTT source stream](sources/mqtt.md) for more detailed info.
- See [SQL](../sqls/overview.md) for more info of XStream SQL.
- Sources can be customized, see [extension](../extension/overview.md) for more detailed info.

### actions

Currently, 2 kinds of actions are supported: [log](sinks/logs.md) and [mqtt](sinks/mqtt.md). Each action can define its own properties.

Actions could be customized to support different kinds of outputs, see [extension](../extension/overview.md) for more detailed info.

### options
The current options includes:

| Option name | Type & Default Value | Description                                                  |
| ------------- | -------- | ------------------------------------------------------------ |
| isEventTime | boolean: false   | Whether to use event time or processing time as the timestamp for an event. If event time is used, the timestamp will be extracted from the payload. The timestamp filed must be specified by the [stream]([extension](../sqls/streams.md)) definition. |
| lateTolerance        | int64:0   | When working with event-time windowing, it can happen that elements arrive late. LateTolerance can specify by how much time(unit is millisecond) elements can be late before they are dropped. By default, the value is 0 which means late elements are dropped.  |