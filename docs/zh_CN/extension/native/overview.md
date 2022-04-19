# 扩展

eKuiper 允许用户自定义扩展，以支持更多的功能。用户可编写插件进行扩展；也可以通过配置的方式，扩展 SQL 中的函数，用于调用外部已有的 REST 或 RPC 服务。

使用插件扩展较为复杂，需要用户编写代码并自行编译，具有一定的开发成本。其使用的场景包括：

- 需要扩展源或是 sink
- 对性能要求较高

使用外部函数扩展，仅需要进行配置，但其需要通过网络进行调用，有一定性能损耗。使用的场景包括：

- 调用已有的服务，如 REST 或 grpc 提供的 AI 服务
- 需要灵活部署的服务

## 插件扩展

eKuiper 允许用户自定义不同类型的扩展。 

- 源扩展用于扩展不同的流源，例如使用来自其他消息服务器的数据。eKuiper 对 [MQTT 消息服务器](../../rules/sources/builtin/mqtt.md)的内置源提供支持。
- Sink/Action 扩展用于将发布/推送数据扩展到不同的目标，例如数据库，其他消息系统，Web 界面或文件系统。eKuiper 中提供内置动作支持，请参阅  [MQTT](../../rules/sinks/builtin/mqtt.md)  & [日志文件](../../rules/sinks/builtin/log.md).。
- 函数扩展允许用户扩展 SQL 中使用的不同函数。 eKuiper支持内置函数，请参见 [函数](../../sqls/built-in_functions.md)。

请阅读以下内容，了解如何实现不同的扩展。

- [源扩展](develop/source.md)
- [Sink/Action 扩展](develop/sink.md)
- [函数扩展](develop/function.md)

## 命名

建议插件名使用 camel case 形式。插件命名有一些限制：
1. 插件输出变量必须为**插件名的首字母大写形式**。 例如，插件名为 _file_ ，则其输出变量名必须为 _File_。
2. _.so_ 文件的名字必须与输出变量名或者插件名相同。例如， _MySource.so_ 或 _mySink.so_。

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

Notice that, the `-trimpath` build flag is required if using the prebuilte kuiper or kuiper docker image because the kuiperd is also built with the flag to improve build reproducibility.

### Plugin development

The development of plugins is to implement a specific interface according to the plugin type and export the implementation with a specific name. There are two types of exported symbol supported:

1. Export a constructor function: Kuiper will use the constructor function to create a new instance of the plugin implementation for each load. So each rule will have one instance of the plugin and each instance will be isolated from others. This is the recommended way.

2. Export an instance: Kuiper will use the instance as singleton for all plugin load. So all rules will share the same instance. For such implementation, the developer will need to handle the shared states to avoid any potential multi-thread problems. This mode is recommended where there are no shared states and the performance is critical. Especially, function extension is usually functional without internal state which is suitable for this mode.


## 状态存储

eKuiper 扩展通过 context 参数暴露了一个基于键值对的状态存储接口，可用于所有类型的扩展，包括 Source，Sink 和 Function 扩展.

状态为键值对，其中键为 string 类型而值为任意数据。键的作用域仅为当前扩展的实例。

用户可通过 context 对象访问状态存储。状态相关方法包括 putState, getState, incrCounter, getCounter and deleteState。

以下代码为函数扩展访问状态的实例。该函数将计算传入的单词数，并将累积数目保存在状态中。

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

### 运行时依赖

有些插件可能需要访问文件系统中的依赖文件。依赖文件建放置于 {{ekuiperPath}}/etc/{{pluginType}}/{{pluginName}} 目录。打包插件时，依赖文件应放置于 [etc 目录](../../operation/restapi/plugins.md#插件文件格式)。安装后，这些文件会自动移动到推荐的位置。

在插件源代码中，开发者可通过 context 获取 eKuiper 根目录，以访问文件系统中的依赖：

```go
ctx.GetRootPath()
```