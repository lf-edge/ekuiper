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

- 源扩展用于扩展不同的流源，例如使用来自其他消息服务器的数据。eKuiper 对 [MQTT 消息服务器](../../guide/sources/builtin/mqtt.md)的内置源提供支持。
- Sink/Action 扩展用于将发布/推送数据扩展到不同的目标，例如数据库，其他消息系统，Web 界面或文件系统。eKuiper 中提供内置动作支持，请参阅  [MQTT](../../guide/sinks/builtin/mqtt.md)  & [日志文件](../../guide/sinks/builtin/log.md).。
- 函数扩展允许用户扩展 SQL 中使用的不同函数。 eKuiper支持内置函数，请参见 [函数](../../sqls/built-in_functions.md)。

请阅读以下内容，了解如何实现不同的扩展。

- [源扩展](develop/source.md)
- [Sink/Action 扩展](develop/sink.md)
- [函数扩展](develop/function.md)

## 命名

建议插件名使用 camel case 形式。插件命名有一些限制：
1. 插件输出变量必须为**插件名的首字母大写形式**。 例如，插件名为 _file_ ，则其输出变量名必须为 _File_。
2. _.so_ 文件的名字必须与输出变量名或者插件名相同。例如， _MySource.so_ 或 _mySink.so_。

### 版本

用户可以**选择**将版本信息添加到 _.so_ 的名称中，以帮助识别插件的版本。然后可以通过 describe CLI 命令或 REST API 检索版本信息。命名约定是在 _@_ 之后的名称中添加一个版本字符串。版本可以是任何字符串。如果版本字符串以 "v" 开头，则返回结果中将忽略 "v" 。下面是一些典型的例子。

- _MySource@v1.0.0.so_ ：版本是 1.0.0
- _MySource@20200331.so_ ：版本是 20200331

如果有多个具有相同名称的插件版本，则只有最新版本(按版本的字符串排序)将生效。

## 插件开发环境设置

需要使用完全相同版本的依赖项来构建插件。并且插件必须实现 Kuiper 导出的接口，所以 Kuiper 项目必须在 gopath 中。

一个典型的开发插件的环境是将插件和 Kuiper 放在同一个项目中。设置环境：
1. 克隆 Kuiper 项目。
2. 根据插件扩展的类型，在 plugins/sources 或者 plugin/sinks 或者 plugin/functions 中创建插件实现的文件。
3. 将文件作为插件构建到同一文件夹中。构建命令通常如下:
```bash
go build -trimpath --buildmode=plugin -o plugins/sources/MySource.so plugins/sources/my_source.go
```

请注意，如果使用预编译的 kuiper 或 kuiper 的 docker 镜像，则需要 `-trimpath` 构建标志，因为 kuiperd 也是使用该标志构建的，这样可以提高构建的可重复性。

### 插件开发

插件的开发就是根据插件类型实现特定的接口，并导出具有特定名称的实现。支持两种类型的导出symbol:

1. 导出一个构造函数：Kuiper 将使用构造函数为每次加载创建一个插件实现的新实例。因此，每条规则将有一个插件实例，并且每个实例都将与其他实例隔离。这是推荐的方式。

2. 导出一个实例：Kuiper 将使用该实例作为所有插件加载的单例。因此，所有规则将共享相同的实例。对于这种实现，开发人员需要处理共享状态，以避免任何潜在的多线程问题。在没有共享状态且性能至关重要的情况下，建议使用此模式。函数扩展通常是没有内部状态的函数，适合这种模式。

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

有些插件可能需要访问文件系统中的依赖文件。依赖文件建放置于 {{ekuiperPath}}/etc/{{pluginType}}/{{pluginName}} 目录。打包插件时，依赖文件应放置于 [etc 目录](../../api/restapi/plugins.md#插件文件格式)。安装后，这些文件会自动移动到推荐的位置。

在插件源代码中，开发者可通过 context 获取 eKuiper 根目录，以访问文件系统中的依赖：

```go
ctx.GetRootPath()
```