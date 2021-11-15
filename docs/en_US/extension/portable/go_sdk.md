# GO SDK for Portable Plugin

By using GO SDK for portable plugins, user can develop portable plugins with go language. The GO SDK provides similar APIs for the source, sink and function extensions. Additionally, it provides a sdk start function as the execution entry point to define the plugin and its symbols.

## Development

### Symbols

As the GO SDK provides almost identical API interfaces, the user's source, sink and function plugin can almost reuse by only some small modifications.

To develop the portable plugin, users need to depend on `github.com/lf-edge/ekuiper/sdk/go` instead of eKuiper main project. Then to implement source, just implement the interfaces in package `github.com/lf-edge/ekuiper/sdk/go/api`.

For source, implement the source interface as below as the same as described in [native plugin source](../native/source.md).

```go
type Source interface {
	// Open Should be sync function for normal case. The container will run it in go func
	Open(ctx StreamContext, consumer chan<- SourceTuple, errCh chan<- error)
	// Configure Called during initialization. Configure the source with the data source(e.g. topic for mqtt) and the properties read from the yaml
	Configure(datasource string, props map[string]interface{}) error
	Closable
}
```

For sink, implement the sink interface as below as the same as described in [native plugin sink](../native/sink.md).

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

For function, implement the function interface as below as the same as described in [native plugin function](../native/function.md).

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

### Plugin Main Program

As the portable plugin is a standalone program, it needs a main program to be able to built into an executable. In go SDK, a start function is provided to define the meta data of the plugin and let it start. A typical main program is as below:

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

Here, in the main function, it calls sdk.Start to start the plugin process. In the argument, a PluginConfig struct is specified to define the plugin name, the sources, functions and sinks name and their initialization functions. This information must match the json file when packaging the plugin.

For the full examples, please check the sdk [example](https://github.com/lf-edge/ekuiper/tree/master/sdk/go/example/mirror).

## Package

We need to prepare the executable file and the json file and then package them. For GO SDK, we need to build the main program into an executable by merely using `go build` like a normal program (it is actually a normal program). Due to go binary file may have different binary name in different os, make sure the file name is correct in the json file. For detail, please check [packaing](./overview.md#package).