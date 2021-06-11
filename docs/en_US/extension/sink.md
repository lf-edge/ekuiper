# Sink Extension

Sink feed data from Kuiper into external systems. Kuiper has built-in sink support for [MQTT broker](../rules/sinks/mqtt.md) and [log sink](../rules/sinks/logs.md). There are still needs to publish data to various external systems include messaging systems and database etc. Sink extension is presented to meet this requirement.

## Developing

### Develop a sink

To develop a sink for Kuiper is to implement [api.Sink](https://github.com/emqx/kuiper/blob/master/xstream/api/stream.go) interface and export it as a golang plugin.

Before starting the development, you must [setup the environment for golang plugin](overview.md#setup-the-plugin-developing-environment). 

To develop a sink, the _Configure_ method must be implemented. This method will be called once the sink is initialized. In this method, a map that contains the configuration in the [rule actions definition](../rules/overview.md#actions) is passed in. Typically, there will be information such as host, port, user and password of the external system. You can use this map to initialize this sink.

```go
//Called during initialization. Configure the sink with the properties from action definition 
Configure(props map[string]interface{}) error
```
The next task is to implement _open_ method. The implementation should be synchronized to create a connection to the external system. A context parameter is provided to retrieve the context information, logger and rule meta information.
```go
//Should be sync function for normal case. The container will run it in go func
Open(ctx StreamContext) error
```  

The main task for a Sink is to implement _collect_ method. The function will be invoked when Kuiper feed any data into the sink. As an infinite stream, this function will be invoked continuously. The task of this function is to publish data to the external system. The first parameter is the context, and the second parameter is the data received from Kuiper.

```go
//Called when each row of data has transferred to this sink
Collect(ctx StreamContext, data interface{}) error
```  

The last method to implement is _Close_ which literally close the connection. It is called when the stream is about to terminate. You could also do any clean up work in this function.

```go
Close(ctx StreamContext) error
```

As the sink itself is a plugin, it must be in the main package. Given the sink struct name is mySink. At last of the file, the sink must be exported as a symbol as below. There are [2 types of exported symbol supported](overview.md#plugin-development). For sink extension, states are usually needed, so it is recommended to export a constructor function.

```go
func MySink() api.Sink {
	return &mySink{}
}
```

The [Memory Sink](https://github.com/emqx/kuiper/blob/master/plugins/sinks/memory/memory.go) is a good example.

### Package the sink
Build the implemented sink as a go plugin and make sure the output so file resides in the plugins/sinks folder.

```bash
go build -trimpath -modfile extensions.mod --buildmode=plugin -o extensions/sinks/MySink.so extensions/sinks/my_sink.go
```

### Usage

The customized sink is specified in a [actions definition](../rules/overview.md#actions). Its name is used as the key of the action. The configuration is the value.

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