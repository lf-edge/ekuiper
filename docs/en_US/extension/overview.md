# Extension

eKuiper allows users to customize extension to support more functions. Users can write plugins for extension by native golang plugin system or by the eKuiper portable plugin system which supports more languages. They can also extend functions in SQL through configuration to call existing external REST or RPC services.

All these 3 extension methods have their own suitable scenarios. Generally, native plugin has the best performance but have the biggest complexity and the least compatibility. While portable plugin has a better balance of performance and complexity. And the external extension does not need coding but has the most performance overhead and only supports function extension. Let's look at each extension method in a nutshell and discuss when to use which one.

## Native Plugin Extension

Native plugin extension leverage the native golang plugin system to dynamically load the custom extensions in runtime. Native plugin was originally supported by eKuiper. But it has a log of limitations due to the golang plugin system itself such as:

- Only support on Linux, FreeBSD and MacOS and hard to work on Alpine linux.
- A plugin is only initialized once, and cannot be closed which means the plugin cannot be unloaded and managed after installed.
- Very harsh requirements to build and deploy which brings a lot of problems in the community. For example, the plugin must be built with the exact same go version, dependency versions etc. with the eKuiper program. Which means, the plugin will always need to be rebuilt when upgrading eKuiper main program.

After installed, the native plugin is actually running like the native code and can share or transfer data in memory with the main program which will guarantee the best performance.

Thus, the native plugin extension fits in scenarios where the user only runs in the supported os and environment, has the ability or infrastructure to rebuild the plugin during update, do not need to unload the plugin in runtime and is only using golang.

## Portable Plugin Extension

Portable plugin extension leverages a plugin system implemented by eKuiper itself based on IPC communication. Potentially, it will support all programming languages. And currently, **go** and **python** are supported. Compared to native plugins, it is portable because the plugins will run in a separate process and do not have those harsh build/deployment requirements.

Portable plugin extension aims to provide the equal functionality with native plugin but support much easier build and deployment. If developers use go, it is even possible to reuse the plugin code with very small modification, and only build and deploy standalone plugins.

Thus, portable plugin extension is a supplement of native plugin. It is suitable to code with multiple programming languages and want to build once and run against all versions.

## External Function Extension

A configuration method is provided that eKuiper can use SQL to directly call external services in a functional manner, including various rpc services, http services, and so on. This method will greatly improve the ease of eKuiper extensions. External functions will be used as a supplement to the plugin system, and plugins are only recommended for high performance requirements.

Take the getFeature function as an example, and suppose an AI service provides getFeature service based on grpc. After eKuiper is configured, you can use the method of `SELECT getFeature(self) from demo` to call the AI service without customizing the plugin.

For detailed configuration method, please refer to [External function](external/external_func.md).

It is useful when the users already have exported services and do not want to write codes. It is a way to easily extend SQL functions in batch.

## Comparison

Let's do some comparisons for all these 3 methods. In the table, *dynamic reload* means if the plugin can be updated or deleted during runtime. *Rebuild for update* means if the plugin needs to be rebuilt when updating the main program. If yes, the version update will become complex. *Separate process* means if the plugin is running standalone from the main program. If yes, the plugin crash won't affect the main program. *Communication* means how to communicate between the main program and the plugin. In memory must be the most efficient method and needs to be run in the same process. IPC requires running in the same machine and has the middle performance and reliance. Web means the communication is transported through web protocol like TCP, it is possible to run in different machines.

| Extension | Extended types         | Need to code? | Language                          | OS                    | Dynamic Reload | Rebuild for update? | Separate Process? | Communication |
|-----------|------------------------|---------------|-----------------------------------|-----------------------|----------------|---------------------|-------------------|---------------|
| Native    | Source, Sink, Function | Yes           | Go                                | Linux, FreeBSD, MacOs | No             | Yes                 | No                | In memory     |
| Portable  | Source, Sink,Function  | Yes           | Go, Python and more in the future | Any                   | Yes            | No                  | Yes               | IPC           |
| External  | Function               | No            | JSON, protobuf                    | Any                   | Yes            | No                  | Yes               | Web           |
