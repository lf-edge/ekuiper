# Portable 插件
作为对原生插件的补充，可移植插件旨在提供相同的功能，同时允许在更通用的环境中运行并由更多语言创建。与原生插件类似，可移植插件也支持自定义 源、目标 和功能扩展。

创建插件的步骤与原生插件类似

1. 使用SDK开发插件。
   1. 通过实现相应的接口来开发每个插件符号（source、sink和function）
   2. 开发主程序，将所有交易品种作为一个插件提供服务
2. 根据编程语言构建或打包插件。
3. 通过eKuiper文件/REST/CLI注册插件

我们的目标是为所有主流语言提供插件. 当前, [go SDK](go_sdk.md) and [python SDK](python_sdk.md) 已经支持.

## 开发

与原生插件不同，portable 插件可以捆绑多个 *Symbol*。每个 Symbol 代表源、汇或功能的扩展。一个符号的实现就是实现类似于原生插件的source、sink或者function的接口。在 portable 插件模式下，就是用选择的语言来实现接口。
然后，用户需要创建一个主程序来定义和服务所有的符号。启动插件时将运行主程序。开发因语言而异，详情请查看 [go SDK](go_sdk.md) 和 [python SDK](python_sdk.md)。

### 调试
我们提供了一个 portable 插件测试服务器来模拟 eKuiper 主程序部分，而开发者可以手动启动插件端以支持调试。
您可以在`tools/plugin_test_server` 中找到该工具。它只支持测试单个插件测试过程。
0. 编辑 testingPlugin 变量以匹配您的插件元数据。
1. 启动此服务器，等待握手。
2. 启动或调试您的插件。确保握手完成。
3. 发出 startSymbol/stopSymbol REST API 来调试您的插件符号。 REST API 是这样的：
   ```
   POST http://localhost:33333/symbol/start
   Content-Type: application/json
   
   {
     "symbolName": "pyjson",
     "meta": {
       "ruleId": "rule1",
       "opId": "op1",
       "instanceId": 1
     },
     "pluginType": "source",
     "config": {}
   }
   ```

## 打包发布

开发完成后，我们需要将结果打包成zip进行安装。在 zip 文件中，文件结构必须遵循以下约定并使用正确的命名：

- {pluginName}.json：文件名必须与插件主程序和REST/CLI命令中定义的插件名相同。
- 插件主程序的可执行文件
- source/sinks/functions 目录：按类别保存所有已定义符号的 json 或 yaml 文件

或者，我们可以打包其他支持文件，如 `install.sh` 和依赖项。

在json文件中，我们需要描述这个插件的元数据。该信息必须与插件主程序中的定义相匹配。下面是一个例子：
```json
{
  "version": "v1.0.0",
  "language": "go",
  "executable": "mirror",
  "sources": [
    "random"
  ],
  "sinks": [
    "file"
  ],
  "functions": [
    "echo"
  ]
}
```
一个插件可以包含多个源、目标和函数，在 json 文件中的相应数组中定义它们。插件必须以单一语言实现，并在 *language* 字段中指定。此外，*executable* 字段需要指定插件主程序可执行文件。请参考[mirror.zip](https://github.com/lf-edge/ekuiper/blob/master/internal/plugin/testzips/portables/mirror.zip) 。

## 管理

通过将内容（json、可执行文件和所有支持文件）放在`plugins/portables/${pluginName}`中，并将配置放在`etc`下的相应目录中，可以在启动时自动加载可移植插件。

要在运行时管理可移植插件，我们可以使用 [REST](../../operation/restapi/plugins.md) 或 [CLI](../../operation/cli/plugins.md) 命令。
## 限制

目前，与原生插件相比，有两个方面的区别：

1. 支持的 Context 方法较少，例如 [State](../native/overview.md#state-storage) ， Connection API 暂不支持；动态参数解析需要开发者自行计算。而 state 计划在未来得到支持。
2. 在函数接口中，参数不能通过AST传递，即用户无法验证参数类型。唯一支持的验证可能是参数计数。在 Sink 接口中，collect 函数的数据类型为 json 编码的 `[]byte`，需要开发者自行解码。