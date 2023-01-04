# 功能

除了核心运行时和 REST API ，其他功能都可通过 [go build constraints](https://pkg.go.dev/go/build#hdr-Build_Constraints) 在编译时打开或者关闭。用户可编译自定义的，仅包含所需功能的二进制包从而减少包的大小，以便能够部署在资源敏感的环境中。

## 功能列表

| 功能                                                                      | Build Tag  | 描述                                                           |
|-------------------------------------------------------------------------|------------|--------------------------------------------------------------|
| 核心                                                                      | core       | eKuiper 的核心运行时。 包括流/表/规则的处理器和 REST API ，配置管理，SQL 解析器，规则运行时等。 |
| [CLI](./operation/cli/overview.md)                                      | rpc        | CLI 服务端                                                      |
| [EdgeX Foundry 整合](./edgex/edgex_rule_engine_tutorial.md)               | edgex      | 内置的 edgeX source, sink 和共享连接支持                               |
| [原生插件](./extension/native/overview.md)                                  | plugin     | 原生插件运行时，REST API和CLI API等                                    |
| [Portable 插件](./extension/portable/overview.md)                         | plugin     | Portable 插件运行时，REST API和CLI API等                             |
| [外部服务](./extension/external/external_func.md)                           | service    | 外部服务运行时，REST API和CLI API等                                    |
| [UI 元数据API](./operation/manager-ui/overview.md)                         | ui         | 元数据的 REST API，通常由 UI 端消费                                     |
| [Prometheus 指标](./operation/config/configuration_file.md#prometheus-配置) | prometheus | 支持发送指标到 prometheus 中                                         |
| [扩展模板函数](./rules/overview.md#模版中支持的函数)                                  | template   | 支持除 go 语言默认的模板函数之外的扩展函数，主要来自 sprig                           |
| [有模式编解码](./rules/codecs.md)                                             | schema     | 支持模式注册及有模式的编解码格式，例如 protobuf                                 |

## Usage

Makefile 里已经提供了三种功能集合：标准，edgeX和核心。标准功能集合包含除了 EdgeX 之外的所有功能。edgeX 功能集合包含了所有的功能；而核心功能集合近包含最小的核心功能。可以通过以下命令，分别编译这三种功能集合：

```shell
# 标准
make
# EdgeX
make build_with_edgex
# 核心
make build_core
```

功能选择通常应用在资源受限的目标环境中。而该环境一般不太适合运行 docker 容易。因此，我们仅提供包含标准及 edgeX 功能集合的 docker 镜像。

若需要自定义功能选择，用户需要自行编译源码。其语法为：

```shell
go build --tags "<FEATURE>"
```

例如，编译带有原生插件功能的核心包，编译命令为：

```shell
go build --tags "core plugin"
```

建议用户以默认 Makefile 为模板，在里面更新编译命令，使其选择所需的 tags ，然后采用 make 命令进行编译。