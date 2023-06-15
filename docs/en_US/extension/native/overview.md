# Native Plugin

eKuiper allows user to customize the different kinds of extensions by the native golang plugin system.

- The source extension is used for extending different stream sources, such as consuming data from other message
  brokers. eKuiper has built-in source support for [MQTT broker](../../guide/sources/builtin/mqtt.md).
- Sink/Action extension is used for extending pub/push data to different targets, such as database, another message
  system, web interfaces or file systems. Built-in action is supported in eKuiper,
  see [MQTT](../../guide/sinks/builtin/mqtt.md) & [log files](../../guide/sinks/builtin/log.md).
- Functions extension allows user to extend different functions that are used in SQL. Built-in functions are supported
  in eKuiper, see [functions](../../sqls/functions/overview.md).

Please read the following to learn how to implement different extensions.

- [Source extension](develop/source.md)
- [Sink/Action extension](develop/sink.md)
- [Function extension](develop/function.md)

## Naming

We recommend plugin name to be camel case. Notice that there are some restrictions for the names:

1. The name of the export symbol of the plugin should be camel case with an **upper case first letter**. It must be the
   same as the plugin name except the first letter. For example, plugin name _file_ must export an export symbol name
   _File_ .
2. The name of _.so_ file must be the same as the export symbol name or the plugin name. For example, _MySource.so_ or
   _mySink.so_.

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
go build -trimpath --buildmode=plugin -o plugins/sources/MySource.so plugins/sources/my_source.go
```

Notice that, the `-trimpath` build flag is required if using the prebuilt eKuiper or eKuiper docker image because the
kuiperd is also built with the flag to improve build reproducibility.

### Plugin development

The development of plugins is to implement a specific interface according to the plugin type and export the implementation with a specific name. There are two types of exported symbol supported:

1. Export a constructor function: Kuiper will use the constructor function to create a new instance of the plugin
   implementation for each load. So each rule will have one instance of the plugin, and each instance will be isolated
   from others. This is the recommended way.

2. Export an instance: eKuiper will use the instance as singleton for all plugin loads. So all rules will share the same
   instance. For such implementation, the developer will need to handle the shared states to avoid any potential
   multi-thread problems. This mode is recommended where there are no shared states and the performance is critical.
   Especially, a function extension is usually functional without internal state which is suitable for this mode.

## State storage

eKuiper extension exposes a key value state storage interface through the context parameter, which can be used for all
types of extensions, including Source/Sink/Function extensions.

States are key-value pairs, where the key is a string, and the value is arbitrary data. Keys are scoped to the current
extended instance.

Users can access the state storage through the context object. State-related methods include putState, getState,
incrCounter, getCounter and deleteState.

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

Some plugins may need to access dependencies in the file system. Those files are put under
{{eKuiperPath}}/etc/{{pluginType}}/{{pluginName}} directory. When packaging the plugin, put those files
in [etc directory](../../api/restapi/plugins.md#plugin-file-format). After installation, they will be moved to the
recommended place.

In the plugin source code, developers can access the dependencies of file system by getting the eKuiper root path from
the context:

```go
ctx.GetRootPath()
```
