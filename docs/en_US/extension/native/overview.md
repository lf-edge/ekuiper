# Go Native Plugin

The Go language plugin system offers a flexible way to extend functionalities. eKuiper allows user to customize the
different kinds of extensions by the native golang plugin system.

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

## Use Cases and Limitations

Extending functionalities using native plugins is relatively complex, requiring users to write code and compile it
themselves, which incurs certain development costs. The scenarios for using native plugins include:

- Needing to extend sources or sinks
- Scenarios with high performance requirements

Due to the limitations of the Go language plugins themselves, the plugin and the main program need to use exactly the
same compilation environment, including but not limited to:

- Go language version
- Dependency module versions and even paths
- Same operating system and CPU architecture

Starting from version v2.0, plugins no longer need to depend on the eKuiper main project, instead, only depend on the
plugin interface sub-project github.com/lf-edge/ekuiper/contract/v2. Therefore, as long as the plugin uses the same Go
language version and contract dependencies as the eKuiper project, it can be used across different eKuiper versions
without the need to recompile for each version.

## Development

To address the limitations of Go language plugins, users need to follow certain guidelines when developing and deploying
extensions, ensuring the correct configuration of the development environment. Plugin development involves implementing
specific interfaces according to the plugin type and exporting implementations with specific names. For more details,
please refer to [Plugin Development](./develop/overview.md).

## How Plugins Work

After completing the plugin development, users can package the .so file and corresponding configuration files into a
zip. Then, they can upload and install the plugin via the API. The installed plugin will save the so file to the file
system, specifically in the `plugins` directory under the corresponding sources/sinks/functions folders. When eKuiper
starts, it will read the corresponding directories under plugins, search for so files, and load the plugins of the
corresponding type based on the folder they are in. If there is an error in the plugin implementation, such as incorrect
interface implementation or mismatched compilation versions, the plugin loading failure information will be written to
the log.

**Note**: The plugin .so file cannot be changed after loading. Updated plugins require a restart of eKuiper to take
effect.

Users can manage plugins through the API for querying and management. For more details, please refer
to [Plugin API](../../api/restapi/plugins.md).
