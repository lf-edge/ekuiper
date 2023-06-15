# Wasm Plugin (beta)

As a complement to the native plugins Wasm plugins are designed to provide the same functionality while allowing to run in a more generic environment and be created by more languages.

The steps to create a plugin are as follows.

1. develop the plugin
2. build or package the plugin according to the programming language
3. register the plugin via eKuiper files/REST/CLI

## Installation Tools

In Wasm plugin mode, implement the function in the language of your choice and compile it into a Wasm file. Any language supported by WebAssembly will do, such as go, rust, etc.
We use the tinygo tool to compile the go file into a Wasm file.

To check if go is installed, run the following command

```shell
go version
```

To check if tinygo is installed, run the following command.

```shell
tinygo version
```

tinygo download address: https://github.com/tinygo-org/tinygo/releases

To check whether wasmedge is installed, please run the following command.

```shell
wasmedge -v
```

wasmedge download location: https://wasmedge.org/book/en/quick_start/install.html

Download command:

```shell
//The easiest way to install WasmEdge is to run the following command. Your system should have git and curl as prerequisites.

curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash

//Run the following command to make the installed binary available in the current session.

source $HOME/.wasmedge/env
```

## Develop Functions

Official tutorial (https://wasmedge.org/book/en/write_wasm/go.html)

Develop fibonacci plugin:

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

Next, compile fibonacci.go into a fibonacci.wasm file

```shell
tinygo build -o fibonacci.wasm -target wasi fibonacci.go
```

Run and get the result, check if it meets the expectation.

```shell
$ wasmedge --reactor fibonacci.wasm fib 10
34
```

## Package

After development is complete, we need to package the results into a zip for installation. In the zip file, the file structure must follow the following conventions and use the correct naming.

- {pluginName}.json: The file name must be the same as the plugin name defined in the main plugin program and REST/CLI commands.
- {pluginName}.wasm: the file name must be the same as the plugin name defined in the plugin main program and REST/CLI commands.

In the json file, we need to describe the metadata of this plugin. This information must match the definition in the main plugin program. The following is an example.

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

## Build eKuiper

The official released eKuiper do not have wasm support, users need build eKuiper by himself

```shell
make build_with_wasm
```

Install the plugin:

```go
bin/kuiper create plugin wasm fibonacci "{\"file\":\"file:///$HOME/ekuiper/internal/plugin/testzips/wasm/fibonacci.zip\"}"
```

Check plugin installation.

```shell
bin/kuiper describe plugin wasm fibonacci
```

## Run

1. Create a stream

    ```shell
    bin/kuiper create stream demo_fib '(num float) WITH (FORMAT="JSON", DATASOURCE="demo_fib")'
    bin/kuiper query
    select fib(num) from demo_fib
    ```

2. Install EMQX to send data.

    ```shell
    docker pull emqx/emqx:v4.0.0
    docker run -d --name emqx -p 1883:1883 -p 8081:8081 -p 8083:8083 -p 8883:8883 -p 8084:8084 -p 18083:18083 emqx/emqx:v4.0.0
    ```

3. Send data by EMQX

Login to: http://127.0.0.1:18083/ with admin/public.

Use TOOLS/Websocket  to send data:

Tpoic    : demo_fib

Messages : {"num" : 25}

Once the message is sent successfully, the terminal receives the execution result.

## Management

By placing the content (json, Wasm files) in `plugins/wasm/${pluginName}`, portable plugins can be loaded automatically at startup.

To manage plugin in runtime, we can use [REST](../../api/restapi/plugins.md) or [CLI](../../api/cli/plugins.md)
