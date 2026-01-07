# Sink Extension

Sink feed data from eKuiper into external systems. eKuiper has built-in sink support
for [MQTT broker](../../../guide/sinks/builtin/mqtt.md) and [log sink](../../../guide/sinks/builtin/log.md) etc. There
are still needs to publish data to various external systems include messaging systems and database etc. Sink extension
is presented to meet this requirement.

**_Note_**: v2.0.0 has modified the sink extension API, which is not fully compatible with the v1.x plugin API. Existing
plugin code needs to be re-adapted.

## Developing

To develop a sink for eKuiper is to
implement [api.Sink](https://github.com/lf-edge/ekuiper/blob/master/contract/api/sink.go) interface and export it as a
golang plugin.

Before starting the development, you
must [setup the environment for golang plugin](./overview.md#setup-the-plugin-developing-environment).

Based on whether the data processed by the Sink is binary, Sinks can be categorized into two types of interfaces:

- `BytesCollector`: Receives binary data encoded by the framework, such as MQTT Sink.
- `TupleCollector`: Receives structured map data, and the plugin needs to encode it itself. For example, SQL Sink.

To develop a Sink, first, you need to confirm which type of Sink the extension belongs to, and then implement the
corresponding type of methods.

### General Methods

All Sink must implement below general methods:

1. To develop a sink, the **Provision** method must be implemented. This method will be called once the sink is
   initialized. In this method, a map that contains the configuration in
   the [rule actions definition](../../../guide/sinks/overview.md) is passed in. Typically, there will be information
   such as host, port, user and password of the external system. You can use this map to initialize this sink.

   ```go
   Provision(ctx StreamContext, configs map[string]any) error
   ```

2. Implement the **Connect** method. This method is used to initialize and establish a connection with the external
   system and is executed only once during rule initialization. The second parameter is used to pass the long-connection
   status to the rule. For example, if the connection implementation automatically reconnects, the reconnection logic
   should run asynchronously to avoid blocking the rule's execution. When the connection logic becomes asynchronous,
   changes in the connection status can be notified to the rule by calling the state change callback function.

   ```go
   Connect(ctx StreamContext, sch StatusChangeHandler) error
   ```

3. Implement specific Collect method according to your sink type. This is the main task for a Sink. The function will be
   invoked when eKuiper feed any data into the sink. As an infinite stream, this function will be invoked continuously.
   The task of this function is to publish data to the external system. The methods implemented by different types of
   Sinks vary slightly. For more details, please refer
   to [Sink Type Implementation](#various-sink-types-implementation).

4. The last method to implement is **Close** which literally close the connection. It is called when the stream is about
   to terminate. You could also do any clean up work in this function.

   ```go
   Close(ctx StreamContext) error
   ```

5. Export the symbol, given the source structure name as `mySink`. At the end of the file, the source must be exported
   as a symbol as follows. There are [two types of export symbols](./overview.md#plugin-development). For source
   extensions, state is usually required, so it is recommended to export the constructor.

The [Memory Sink](https://github.com/lf-edge/ekuiper/blob/master/extensions/sinks/memory/memory.go) is a good example.

### Various Sink Types Implementation

Based on the type of data being sent, Sinks can be categorized into two types, and users can implement
different `Collect` methods for each.

- `BytesCollector`: Implement the `Collect` method to handle the `RawTuple` sent by the upstream operator. Users can
  obtain the encoded binary data for processing via `RawTuple.Raw()`. Refer to the MQTT Sink implementation for an
  example.

  ```go
  Collect(ctx StreamContext, item RawTuple) error
  ```

- `TupleCollector`: Implement the `Collect` and `CollectList` methods to handle the `Tuple` or `Tuple List` sent by the
  upstream operator. Refer to the SQL Sink implementation for an example.

  ```go
  Collect(ctx StreamContext, item MessageTuple) error
  CollectList(ctx StreamContext, items MessageTupleList) error
  ```

The `Collect` method implementation can return any type of error. However, if you want the automatic retry mechanism to
take effect, the returned error message must start with "io error". In most cases, only IO issues require retries.

### Updatable Sink

If your sink is updatable, you'll need to deal with the `rowkindField` property. Some sink may also need a `keyField`
property to specify which field is the primary key to update.

So in the **Provision** method, parse the `rowkindField` to know which field in the data is the update action. Then in
the
**Collect** method, retrieve the rowkind by the `rowkindField` and perform the proper action. The rowkind value could
be `insert`, `update`, `upsert` and `delete`. For example, in SQL sink, each rowkind value will generate different SQL
statement to execute.

### Parsing Dynamic Properties

In a custom sink plugin, users may still want to
support [dynamic properties](../../../guide/sinks/overview.md#dynamic-properties) like built-in sinks. The `Collect`
method passes in a `Tuple` that contains the parsed dynamic values. The development team should design which properties
support dynamic values based on the business logic. Then, when writing the code, use the following method to parse the
attribute values passed in by the user.

```go
func Collect(ctx StreamContext, item RawTuple) error {
if dp, ok := item.(api.HasDynamicProps); ok {
temp, transformed := dp.DynamicProps("propName")
if transformed {
tpc = temp
}
}
}
```

## Usage

The customized sink is specified in [actions definition](../../../guide/sinks/overview.md). Its name is used as the key of the action. The configuration is the value.

If you have developed a sink implementation MySink, you should have:

1. In the plugin file, symbol MySink is exported.
2. The compiled MySink.so file is located inside **plugins/sink**

To use it, define the action mySink inside a rule definition:

```json
{
  "id": "rule1",
  "sql": "SELECT demo.temperature, demo1.temp FROM demo left join demo1 on demo.timestamp = demo1.timestamp where demo.temperature > demo1.temp GROUP BY demo.temperature, HOPPINGWINDOW(ss, 20, 10)",
  "actions": [
    {
      "mySink": {
        "server": "tcp://47.52.67.87:1883",
        "topic": "demoSink"
      }
    }
  ]
}
```

Whereas, **mySink** is a key of the actions. The value of mySink is the properties for that sink.
