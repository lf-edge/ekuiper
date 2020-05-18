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

Notice that, there are some restrictions for the names:
1. The name of _.so_ file must be camel case with an upper case first letter. For example, _MySource.so_ or _MySink.so_.
2. The name of the export symbol of the plugin must be camel case with an upper case first letter.

### Version

The user can **optionally** add a version string to the name of _.so_ to help identify the version of the plugin. The version can be then retrieved through describe CLI command or REST API. The naming convention is to add a version string to the name after _@v_. The version can be any string. Below are some typical examples.

- _MySource@v1.0.0.so_ : version is 1.0.0
- _MySource@v20200331.so_:  version is 20200331

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



Please read below for how to realize the different extensions.

- [Source extension](source.md)
- [Sink/Action extension](sink.md)
- [Function extension](function.md)

