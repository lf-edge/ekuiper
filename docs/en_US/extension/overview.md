# Extension

eKuiper allows users to customize extension to support more functions. Users can write plugins for extension. They can also extend functions in SQL through configuration to call existing external REST or RPC services.

Extension by the use of plugin is more complex and requires users to write code and compile by themselves, which has a certain development cost. The scenarios used include:

- Need to extend the source or sink
- With high performance requirements

Extension by the use of external function requires only configuration, but it needs to be called through the network, which has a certain performance loss. The scenarios used include:

- Call existing services, such as AI services provided by REST or grpc
- Services that require flexible deployment

## Plugin extension

eKuiper allows user to customize the different kinds of extensions.  

- The source extension is used for extending different stream source, such as consuming data from other message brokers. eKuiper has built-in source support for [MQTT broker](../rules/sources/mqtt.md).
- Sink/Action extension is used for extending pub/push data to different targets, such as database, other message system, web interfaces or file systems. Built-in action is supported in eKuiper, see [MQTT](../rules/sinks/mqtt.md) & [log files](../rules/sinks/logs.md).
- Functions extension allows user to extend different functions that used in SQL. Built-in functions is supported in eKuiper, see [functions](../sqls/built-in_functions.md).

Please read the following to learn how to implement different extensions.

- [Source extension](./source.md)
- [Sink/Action extension](./sink.md)
- [Function extension](./function.md)

## Naming

We recommend plugin name to be camel case. Notice that, there are some restrictions for the names:

1. The name of the export symbol of the plugin should be camel case with an **upper case first letter**. It must be the same as the plugin name except the first letter. For example, plugin name _file_ must export a export symbol name _File_ .
2. The name of _.so_ file must be the same as the export symbol name or the plugin name. For example, _MySource.so_ or _mySink.so_.

### State storage

eKuiper extension exposes a key value state storage interface through the context parameter, which can be used for all types of extensions, including Source/Sink/Function extensions.

States are key-value pairs, where the key is a string and the value is arbitrary data. Keys are scoped the to current extended instance.

Users can access the state storage through the context object. State-related methods include putState, getState, incrCounter, getCounter and deleteState.

Below is an example of a function extension to access states. This function will count the number of words passed in and save the cumulative number in the state.

```go
func (f *accumulateWordCountFunc) Exec(args []interface{}, ctx api.FunctionContext) (interface{}, bool) {
    logger := ctx.GetLogger()    
	err := ctx.IncrCounter("allwordcount", len(strings.Split(args[0], args[1])))
	if err != nil {
		return err, false
	}
	if c, err := ctx.GetCounter("allwordcount"); err != nil   {
		return err, false
	} else {
		return c, true
	}
}
```

### Runtime dependencies

Some plugin may need to access dependencies in the file system. Those files is put under {{eKuiperPath}}/etc/{{pluginType}}/{{pluginName}} directory. When packaging the plugin, put those files in [etc directory](../restapi/plugins.md#plugin-file-format). After installation, they will be moved to the recommended place.

In the plugin source code, developers can access the dependencies of file system by getting the eKuiper root path from the context:

```go
ctx.GetRootPath()
```

## External function extension

A configuration method is provided that eKuiper can use SQL to directly call external services in a functional manner, including various rpc services, http services, and so on. This method will greatly improve the ease of eKuiper extensions. External functions will be used as a supplement to the plugin system, and plugins are only recommended for high performance requirements.

Take the getFeature function as an example, and suppose an AI service provides getFeature service based on grpc. After eKuiper is configured, you can use the method of `SELECT getFeature(self) from demo` to call the AI service without customizing the plugin.

For detailed configuration method, please refer to [External function](external_func.md).

