# Go 原生插件扩展

Go 语言插件系统提供了一种灵活的扩展功能的方式。eKuiper 允许用户采用 Go 语言原生插件系统自定义扩展，以支持更多的功能。

- 源扩展用于扩展不同的流源，例如使用来自其他消息服务器的数据。eKuiper 对 [MQTT 消息服务器](../../guide/sources/builtin/mqtt.md)的内置源提供支持。
- Sink/Action 扩展用于将发布/推送数据扩展到不同的目标，例如数据库，其他消息系统，Web 界面或文件系统。eKuiper
  中提供内置动作支持，请参阅 [MQTT](../../guide/sinks/builtin/mqtt.md) & [日志文件](../../guide/sinks/builtin/log.md)。
- 函数扩展允许用户扩展 SQL 中使用的不同函数。 eKuiper支持内置函数，请参见 [函数](../../sqls/functions/overview.md)。

请阅读以下内容，了解如何实现不同的扩展。

- [源扩展](develop/source.md)
- [Sink/Action 扩展](develop/sink.md)
- [函数扩展](develop/function.md)

## 使用场景和限制

使用插件扩展较为复杂，需要用户编写代码并自行编译，具有一定的开发成本。其使用的场景包括：

- 需要扩展源或是 sink
- 对性能要求较高的场景

由于 Go 语言插件本身的限制，插件与主程序需要使用完全相同的编译环境，包括但不限于：

- Go 语言版本
- 依赖库版本甚至是路径
- 相同的操作系统及 CPU 架构

在 v2.0 之后的版本，插件不再必须依赖于 eKuiper 主项目，仅依赖插件接口子项目 `github.com/lf-edge/ekuiper/contract/v2`
。因此，插件只要与 eKuiper 项目使用相同的 Go 语言版本和 contract 依赖即可在不同的 eKuiper 版本通用。无需每个版本重新编译。

## 开发

为了应对 Go
语言插件的限制，用户进行扩展开发和部署时需要遵循一定的规范，正确配置开发环境。插件的开发就是根据插件类型实现特定的接口，并导出具有特定名称的实现。详情请参考[插件开发](./develop/overview.md)。

## 运行原理

插件开发完成后，用户可打包 so 文件及对应配置文件为 zip 。然后通过 API 上传并安装插件。安装完的插件会将 so
文件保存到文件系统中，即 `plugins` 目录对应的 sources/sinks/functions 文件夹中。eKuiper 启动时，会读取 plugins 下对应的目录，搜寻
so 文件，并根据所在文件夹的类型载入对应类型的插件。若插件实现有错误，例如实现的接口不正确，编译的版本不匹配等，插件载入失败信息会写入到日志中。

**请注意**：插件 so 载入后不可变更。更新的插件需要重启 eKuiper 才能生效。

用户可以通过 API 进行插件的查询管理。详情请参考[插件 API](../../api/restapi/plugins.md)。
