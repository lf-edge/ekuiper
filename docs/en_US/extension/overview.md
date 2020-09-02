# Extensions

Kuiper allows user to customize the different kinds of extensions.  

- The source extension is used for extending different stream source, such as consuming data from other message brokers. Kuiper has built-in source support for [MQTT broker](../rules/sources/mqtt.md).
- Sink/Action extension is used for extending pub/push data to different targets, such as database, other message system, web interfaces or file systems. Built-in action support in Kuiper, see [MQTT](../rules/sinks/mqtt.md) & [log files](../rules/sinks/logs.md).
- Functions extension allows user to extend different functions that used in SQL. Built-in functions supported in Kuiper, see [functions](../sqls/built-in_functions.md).

Kuiper extensions are based on golang plugin system. The general steps to make extensions are:
1. Create the plugin package that implements required source, sink or function interface.
2. Compile the plugin into a _.so_ file, and put it into sources or sinks or functions folder under _plugins_ folder.

Currently golang plugins are only supported on Linux and macOS which poses the same limitation for Kuiper extensions.

## Naming

We recommend plugin name to be camel case. Notice that, there are some restrictions for the names:
1. The name of the export symbol of the plugin should be camel case with an **upper case first letter**. It must be the same as the plugin name except the first letter. For example, plugin name _file_ must export a symbol _File_.
2. The name of _.so_ file must be the same as the export symbol name or the plugin name. For example, _MySource.so_ or _mySink.so_.

### Version

The user can **optionally** add a version string to the name of _.so_ to help identify the version of the plugin. The version can be then retrieved through describe CLI command or REST API. The naming convention is to add a version string to the name after _@_. The version can be any string. If the version string starts with "v", the "v" will be ignored in the return result. Below are some typical examples.

- _MySource@v1.0.0.so_ : version is 1.0.0
- _MySource@20200331.so_:  version is 20200331

If multiple versions of plugins with the same name in place, only the latest version(ordered by the version string) will be taken effect.

## Setup the plugin developing environment
It is required to build the plugin with exactly the same version of dependencies. And the plugin must implement interfaces exported by Kuiper, so the Kuiper project must be in the gopath. 

A typical environment for developing plugins is to put the plugin and Kuiper in the same project. To set it up:
1. Clone Kuiper project.
2. Create the plugin implementation file inside plugins/sources or plugin/sinks or plugin/functions according to what extension type is developing.
3. Build the file as plugin into the same folder. The build command is typically like:
```bash
go build --buildmode=plugin -o plugins/sources/MySource.so plugins/sources/my_source.go
```


### Plugin development
The development of plugins is to implement a specific interface according to the plugin type and export the implementation with a specific name. There are two types of exported symbol supported:

1. Export a constructor function: Kuiper will use the constructor function to create a new instance of the plugin implementation for each load. So each rule will have one instance of the plugin and each instance will be isolated from others. This is the recommended way.

2. Export an instance: Kuiper will use the instance as singleton for all plugin load. So all rules will share the same instance. For such implementation, the developer will need to handle the shared states to avoid any potential multi-thread problems. This mode is recommended where there are no shared states and the performance is critical. Especially, function extension is usually functional without internal state which is suitable for this mode.

Please read below for how to realize the different extensions.

- [Source extension](source.md)
- [Sink/Action extension](sink.md)
- [Function extension](function.md)

### State Storage

Kuiper extensions export a key value state storage interface for Source/Sink/Function through the context.

States are key-value pairs, where the key is a string and the value is arbitrary data. Keys are scoped to an individual extension.

You can access states within extensions using the putState, getState, incrCounter, getCounter and deleteState calls on the context object.

Below is an example of a function extension to access states. It will record the accumulate word count across a range of function calls.

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

The state storage API includes

```go
/**
 * Increase the builtin distributed counter referred by key
 * @param key The name of the key
 * @param amount The amount to be incremented
 * @return error if any 
 */
IncrCounter(key string, amount int) error
/**
 * Retrieve the counter value by key
 * @param key The name of the key
 * @return the counter value
 * @return error if any 
 */
GetCounter(key string) (int, error)
/**
 * Set or update the state value for the key.
 *
 * @param key name of the key
 * @param value state value of the key
 * @return error if any 
 */
PutState(key string, value interface{}) error
/**
 * Retrieve the state value for the key.
 *
 * @param key name of the key
 * @return the state value
 * @return error if any 
 */
GetState(key string) (interface{}, error)
/**
 * Delete the state value for the key.
 *
 * @param key name of the key
 * @return error if any 
 */
DeleteState(key string) error
```

#### State data type

The state can be any type. If the rule [checkpoint mechanism](../rules/state_and_fault_tolerance.md) is enabled, the state will be serialized by [golang gob](https://golang.org/pkg/encoding/gob/). So it is required to be gob compatibile. For custom data type, register the type by ``gob.Register(value interface{})`` .