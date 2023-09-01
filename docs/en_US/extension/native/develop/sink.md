# Sink Extension

Sink feed data from eKuiper into external systems. eKuiper has built-in sink support for [MQTT broker](../../../guide/sinks/builtin/mqtt.md) and [log sink](../../../guide/sinks/builtin/log.md). There are still needs to publish data to various external systems include messaging systems and database etc. Sink extension is presented to meet this requirement.

## Developing

### Develop a sink

To develop a sink for eKuiper is to implement [api.Sink](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) interface and export it as a golang plugin.

Before starting the development, you must [setup the environment for golang plugin](../overview.md#setup-the-plugin-developing-environment).

To develop a sink, the _Configure_ method must be implemented. This method will be called once the sink is initialized. In this method, a map that contains the configuration in the [rule actions definition](../../../guide/sinks/overview.md) is passed in. Typically, there will be information such as host, port, user and password of the external system. You can use this map to initialize this sink.

```go
//Called during initialization. Configure the sink with the properties from action definition
Configure(props map[string]interface{}) error
```

The next task is to implement _open_ method. The implementation should be synchronized to create a connection to the external system. A context parameter is provided to retrieve the context information, logger and rule meta information.

```go
//Should be sync function for normal case. The container will run it in go func
Open(ctx StreamContext) error
```

The main task for a Sink is to implement _collect_ method. The function will be invoked when eKuiper feed any data into the sink. As an infinite stream, this function will be invoked continuously. The task of this function is to publish data to the external system. The first parameter is the context, and the second parameter is the data received from eKuiper. The data could be 2 types:

1. Map slice `[]map[string]interface{}`: this is the default data type.
2. Map `map[string]interface{}`: this is a possible data type when the `sendSingle` property is set.

Most of the time, the map content will be the selective fields. But if `sendError` property is enabled and there are errors happen in the rule, the map content will be like `{"error":"error message here"}`.

The developer can use two methods to obtain the transformed data: `ctx.TransformOutput(data)` from the context method and `TransItem(data, dataField, fields)` from the transform package.

- `ctx.TransformOutput(data)`：
  - parameter
    - `data`: the input data, with a type of interface{}.
  - return
    - the transformed data as a byte array ([]byte).
    - a boolean value indicating whether the data was transformed. If it is false, it means that the result was not transformed and the original value was returned.
    - the error message (error).
  - process: The input data is transformed based on the dataTemplate, dataField, and fields properties, and returned as a byte array. If the [`dataTemplate` property](../../../guide/sinks/data_template.md) is set, the method first formats the input data according to the template. If neither dataField nor fields are set, the formatted data is returned as a byte array. Otherwise, the formatted data is converted to structured data, and the dataField and fields properties are used to extract the desired data. Finally, the transformed data is encoded as a byte array and returned.
- `TransItem(data, dataField, fields)`：
  - parameter
    - `data`: the input data, with a type of interface{}.
    - `dataField`: specify which data to extract, with a type of string. See details in[`dataField` property](../../../guide/sinks/overview.md#common-properties).
    - `fields`: select the fields of the output message, with a type of []string. See details in[`fields` property](../../../guide/sinks/overview.md#common-properties).
  - return
    - the transformed data(interface{}).
    - a boolean value indicating whether the data was transformed. If it is false, it means that the result was not transformed and the original value was returned.
    - the error message (error).
  - process: `TransItem(data, dataField, fields)` transforms the input data based on the dataField and fields properties, and returns it as structured data. If the dataField property is set, the method first extracts nested data through the dataField property. Then, if the fields property is set, the method selects the desired fields from the extracted data. Finally, the transformed data is returned.

The developer can return any errors. However, to leverage the retry feature of eKuiper, the developer must return an error whose message starts with "io error".

```go
//Called when each row of data has transferred to this sink
Collect(ctx StreamContext, data interface{}) error
```

The last method to implement is _Close_ which literally close the connection. It is called when the stream is about to terminate. You could also do any clean up work in this function.

```go
Close(ctx StreamContext) error
```

As the sink itself is a plugin, it must be in the main package. Given the sink struct name is mySink. At last of the file, the sink must be exported as a symbol as below. There are [2 types of exported symbol supported](../overview.md#plugin-development). For sink extension, states are usually needed, so it is recommended to export a constructor function.

```go
func MySink() api.Sink {
return &mySink{}
}
```

The [Memory Sink](https://github.com/lf-edge/ekuiper/blob/master/extensions/sinks/memory/memory.go) is a good example.

#### Updatable Sink

If your sink is updatable, you'll need to deal with the `rowkindField` property. Some sink may also need a `keyField`
property to specify which field is the primary key to update.

So in the _Configure_ method, parse the `rowkindField` to know which field in the data is the update action. Then in the
_Collect_ method, retrieve the rowkind by the `rowkindField` and perform the proper action. The rowkind value could
be `insert`, `update`, `upsert` and `delete`. For example, in SQL sink, each rowkind value will generate different SQL
statement to execute.

#### Customize Resend Strategy

Sink can set the [cache and resend strategy](../../../guide/sinks/overview.md#caching) to ensure data delivery.
By default, resending data will invoke the `Collect` method again.
If you want to customize the resend strategy, you can implement the `CollectResend` method in the sink.
In that method, you can do some format conversion or other operations on the data.
You can also parse the common sink property `resendDestination` and make it the destination of the resend data.
For example, you can resend the data to another topic defined in that property.

```go
// CollectResend Called when the sink cache resend is triggered
CollectResend(ctx StreamContext, data interface{}) error
```

#### Parse dynamic properties

For customized sink plugins, users may still want to
support [dynamic properties](../../../guide/sinks/overview.md#dynamic-properties) like the built-in ones.

In the context object, a function `ParseTemplate` is provided to support the parsing of the dynamic property with the go
template syntax. In the customized sink, developers can specify some properties to be dynamic according to the business
logic. And in the plugin code, use this function to parse the user input in the collect function or elsewhere.

```go
// Parse the prop of go template syntax against the current data.
value, err := ctx.ParseTemplate(s.prop, data)
// Use the parsed value for the following business logic.
```

### Package the sink

Build the implemented sink as a go plugin and make sure the output so file resides in the plugins/sinks folder.

```bash
go build -trimpath --buildmode=plugin -o extensions/sinks/MySink.so extensions/sinks/my_sink.go
```

### Usage

The customized sink is specified in [actions definition](../../../guide/sinks/overview.md). Its name is used as the key of the action. The configuration is the value.

If you have developed a sink implementation MySink, you should have:

1. In the plugin file, symbol MySink is exported.
2. The compiled MySink.so file is located inside _plugins/sinks_

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

Whereas, _mySink_ is a key of the actions. The value of mySink is the properties for that sink.
