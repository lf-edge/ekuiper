# Wasm 插件 (beta)

作为对原生插件的补充  Wasm 插件旨在提供相同的功能，同时允许在更通用的环境中运行并由更多语言创建。

创建插件的步骤如下：

1. 开发插件
2. 根据编程语言构建或打包插件
3. 通过 eKuiper 文件/REST/CLI 注册插件

## 安装工具

在 Wasm 插件模式下，用选择的语言来实现函数，并将其编译成 Wasm 文件。只要是 WebAssembly 支持的语言均可，例如 go，rust 等。
我们使用 tinygo 工具将 go 文件编译成 Wasm 文件。

检查 go 是否已经安装，执行以下命令

```shell
go version
```

检查 tinygo 是否已经安装，请运行以下命令。

```shell
tinygo version
```

tinygo 下载地址 : <https://github.com/tinygo-org/tinygo/releases>

检查 wasmedge 是否已经安装，请运行以下命令。

```shell
wasmedge -v
```

官方文档 wasmedge 下载地址 : <https://wasmedge.org/book/en/quick_start/install.html>

下载命令:

```shell
//The easiest way to install WasmEdge is to run the following command. Your system should have git and curl as prerequisites.

curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash

//Run the following command to make the installed binary available in the current session.

source $HOME/.wasmedge/env
```

## 开发函数

官方教程(<https://wasmedge.org/book/en/write_wasm/go.html>)

开发 fibonacci 插件(相比官方，略有改动)

fibonacci.go

```go
package main

func main() {
}

//export fib
func fibArray(n int32) int32 {
    arr := make([]int32, n)
    for i := int32(0); i < n; i++ {
        switch {
        case i < 2:
            arr[i] = i
        default:
            arr[i] = arr[i-1] + arr[i-2]
        }
    }
    return arr[n-1]
}
```

接下来将 fibonacci.go 编译成 fibonacci.wasm 文件

```shell
tinygo build -o fibonacci.wasm -target wasi fibonacci.go
```

运行并得到结果,检查是否符合预期.

```shell
$ wasmedge --reactor fibonacci.wasm fib 10
34
```

## 打包发布

开发完成后，我们需要将结果打包成 zip 进行安装。在 zip 文件中，文件结构必须遵循以下约定并使用正确的命名：

- {pluginName}.json：文件名必须与插件主程序和 REST/CLI 命令中定义的插件名相同。
- {pluginName}.wasm：文件名必须与插件主程序和 REST/CLI 命令中定义的插件名相同。

在json文件中，我们需要描述这个插件的元数据。该信息必须与插件主程序中的定义相匹配。下面是一个例子：

fibonacci.json

```json
{
  "version": "v1.0.0",
  "functions": [
    "fib"
  ],
  "wasmEngine": "wasmedge"
}
```

## 编译 eKuiper

目前官方发布的 eKuiper 并不支持 wasm, 用户需要自行编译。

```shell
make build_with_wasm
```

安装插件：

首先启动服务器

```shell
bin/kuiperd
```

然后创建插件

```go
bin/kuiper create plugin wasm fibonacci "{\"file\":\"file:///$HOME/ekuiper/internal/plugin/testzips/wasm/fibonacci.zip\"}"
```

$HOME 为 自己本机路径，输入以下命令查看

```shell
$HOME
```

后面所使用的fibonacci.zip文件已提供，如果是自己开发新的插件，修改为新插件的绝对路径地址即可。

查询插件信息

```shell
bin/kuiper describe plugin wasm fibonacci
```

## 运行

1. 创建流

   ```shell
   bin/kuiper create stream demo_fib '(num float) WITH (FORMAT="JSON", DATASOURCE="demo_fib")'
   
   bin/kuiper query
   
   select fib(num) from demo_fib
   ```

2. 安装 emqx docker容器并运行

   ```shell
   docker pull emqx/emqx:v4.0.0
   docker run -d --name emqx -p 1883:1883 -p 8081:8081 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx:v4.0.0
   ```

3. 登陆网页

   地址: http://127.0.0.1:18083/

   登陆账户/密码：admin/public

   使用 TOOLS/Websocket 工具发送数据:

   Tpoic    : demo_fib

   Messages : {"num" : 25}

   消息发送成功后，终端即可接收到执行结果.

## 管理

通过将内容（json、Wasm文件）放在 `plugins/wasm/${pluginName}` 中，可以在启动时自动加载可移植插件。

要在运行时管理可移植插件，我们可以使用 [REST](../../api/restapi/plugins.md) 或 [CLI](../../api/cli/plugins.md) 命令。
