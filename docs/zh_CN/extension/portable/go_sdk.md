# Portable 插件 Go SDK

用户可利用 GO SDK 来开发 portable 插件，这个 SDK 提供了类似原生插件的 API，另外它提供了启动函数，用户只需填充插件信息即可。

## 插件开发

### Symbols

由于 portable 插件 GO SDK 提供了类似原生插件的API，用户做简单的修改即可复用以前编写的原生插件

用户只需依赖 `github.com/lf-edge/ekuiper/sdk/go` 而不是 eKuiper 主项目即可编写 portable 插件，用户需要实现 `github.com/lf-edge/ekuiper/sdk/go/api` 中的相应接口即可

对于源，实现跟[原生源插件](../native/develop/source.md)中一样的接口即可 

```go
type Source interface {
	// Open Should be sync function for normal case. The container will run it in go func
	Open(ctx StreamContext, consumer chan<- SourceTuple, errCh chan<- error)
	// Configure Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties read from the yaml
	Configure(datasource string, props map[string]interface{}) error
	Closable
}
```

对于目标，实现跟[原生目标插件](../native/develop/sink.md)中一样的接口即可

```go
type Sink interface {
	//Should be sync function for normal case. The container will run it in go func
	Open(ctx StreamContext) error
	//Called during initialization. Configure the sink with the properties from rule action definition
	Configure(props map[string]interface{}) error
	//Called when each row of data has transferred to this sink
	Collect(ctx StreamContext, data interface{}) error
	Closable
}
```

对于函数，实现跟[原生函数插件](../native/develop/function.md)中一样的接口即可

```go
type Function interface {
	//The argument is a list of xsql.Expr
	Validate(args []interface{}) error
	//Execute the function, return the result and if execution is successful.
	//If execution fails, return the error and false.
	Exec(args []interface{}, ctx FunctionContext) (interface{}, bool)
	//If this function is an aggregate function. Each parameter of an aggregate function will be a slice
	IsAggregate() bool
}
```

### 插件主程序
由于 portable 插件是一个独立的程序，需要编写成一个可执行程序。在 GO SDK 中, 提供了启动函数，用户只需填充插件信息即可。启动函数如下：

```go
package main

import (
	"github.com/lf-edge/ekuiper/sdk/go/api"
	sdk "github.com/lf-edge/ekuiper/sdk/go/runtime"
	"os"
)

func main() {
	sdk.Start(os.Args, &sdk.PluginConfig{
		Name: "mirror",
		Sources: map[string]sdk.NewSourceFunc{
			"random": func() api.Source {
				return &randomSource{}
			},
		},
		Functions: map[string]sdk.NewFunctionFunc{
			"echo": func() api.Function {
				return &echo{}
			},
		},
		Sinks: map[string]sdk.NewSinkFunc{
			"file": func() api.Sink {
				return &fileSink{}
			},
		},
	})
}
```
在主函数中调用了 `sdk.Start`来启动插件进程。在参数中，`PluginConfig` 定义了插件名字，源，目标，函数构造函数。注意这些信息必须跟插件安装包中的 json 描述文件一致

完整例子请参考这个[例子](https://github.com/lf-edge/ekuiper/tree/master/sdk/go/example/mirror)

## 打包发布
我们需要将可执行文件和 json 描述文件一起打包，使用 GO SDK，仅仅需要 `go build`编译出可执行文件即可。由于在不同操作系统下编译出到的可执行文件名字有所不同，需要确保 json 描述文件中可执行文件名字的准确性。详细信息，请[参考](./overview.md#package)