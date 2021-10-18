# Native Plugin

eKuiper allows user to customize the different kinds of extensions by the native golang plugin system. 

- The source extension is used for extending different stream source, such as consuming data from other message brokers. eKuiper has built-in source support for [MQTT broker](../../rules/sources/mqtt.md).
- Sink/Action extension is used for extending pub/push data to different targets, such as database, other message system, web interfaces or file systems. Built-in action is supported in eKuiper, see [MQTT](../../rules/sinks/mqtt.md) & [log files](../../rules/sinks/logs.md).
- Functions extension allows user to extend different functions that used in SQL. Built-in functions is supported in eKuiper, see [functions](../../sqls/built-in_functions.md).

Please read the following to learn how to implement different extensions.

- [Source extension](./source.md)
- [Sink/Action extension](./sink.md)
- [Function extension](./function.md)

## Naming

We recommend plugin name to be camel case. Notice that, there are some restrictions for the names:

1. The name of the export symbol of the plugin should be camel case with an **upper case first letter**. It must be the same as the plugin name except the first letter. For example, plugin name _file_ must export a export symbol name _File_ .
2. The name of _.so_ file must be the same as the export symbol name or the plugin name. For example, _MySource.so_ or _mySink.so_.

## State storage

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

## Runtime dependencies

Some plugin may need to access dependencies in the file system. Those files is put under {{eKuiperPath}}/etc/{{pluginType}}/{{pluginName}} directory. When packaging the plugin, put those files in [etc directory](../../restapi/plugins.md#plugin-file-format). After installation, they will be moved to the recommended place.

In the plugin source code, developers can access the dependencies of file system by getting the eKuiper root path from the context:

```go
ctx.GetRootPath()
```

## Parse dynamic properties

For customized sink plugins, users may still want to support [dynamic properties](../../rules/overview.md#dynamic-properties) like the built-in ones. 

In the context object, a function `ParseDynamicProp` is provided to support the parsing of the dynamic property syntax. In the customized sink, developers can specify some properties to be dynamic according to the business logic. And in the plugin code, use this function to parse the user input in the collect function or elsewhere.

```go
// Parse the prop of jsonpath syntax against the current data.
value, err := ctx.ParseDynamicProp(s.prop, data)
// Use the parsed value for the following business logic.
```