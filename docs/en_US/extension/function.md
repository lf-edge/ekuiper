# Function Extension

In the eKuiper SQL syntax, [many built-in functions](../sqls/built-in_functions.md) are provided to server for various reusable business logic. However, the users still likely need various reusable business logic which are not covered by the built ins. The function extension is presented to customized the functions.

## Developing

### Develop a customized function

To develop a function for eKuiper is to implement [api.Function](https://github.com/lf-edge/ekuiper/blob/master/xstream/api/stream.go) interface and export it as a golang plugin.

Before starting the development, you must [setup the environment for golang plugin](overview.md#setup-the-plugin-developing-environment). 

To develop a function, the _Validate_ method is firstly to be implemented. This method will be called during SQL validation. In this method, a slice of [xsql.Expr](https://github.com/lf-edge/ekuiper/blob/master/xsql/ast.go) is passed as the parameter that contains the arguments for this function in the runtime. The developer can do a validation against it to check the argument count and type etc. If validation is successful, return nil. Otherwise, return an error object.

```go
//The argument is a list of xsql.Expr
Validate(args []interface{}) error
```
There are 2 types of functions: aggregate function and common function. For aggregate function, if the argument is a column, the received value will always be a slice of the column values in a group. The extended function must distinguish the function type by implement _IsAggregate_ method.

```go
//If this function is an aggregate function. Each parameter of an aggregate function will be a slice
IsAggregate() bool
```

The main task for a Function is to implement _exec_ method. The method will be leverage to calculate the result of the function in the SQL. The argument is a slice of the values for the function parameters. You can use them to do the calculation. If the calculation is successful, return the result and true; otherwise, return nil and false. 

```go
//Execute the function, return the result and if execution is successful.If execution fails, return the error and false. 
Exec(args []interface{}) (interface{}, bool)
```  

As the function itself is a plugin, it must be in the main package. Given the function struct name is myFunction. At last of the file, the source must be exported as a symbol as below. There are [2 types of exported symbol supported](overview.md#plugin-development). For function extension, if there is no internal state, it is recommended to export a singleton instance.

```go
var MyFunction myFunction
```

The [Echo Function](https://github.com/lf-edge/ekuiper/blob/master/plugins/functions/echo/echo.go) is a good example.

### Export multiple functions

In one plugin, developers can export multiple functions. Each function must implement [api.Function](https://github.com/lf-edge/ekuiper/blob/master/xstream/api/stream.go) as described at [Develop a customized function](#develop-a-customized-function) section. Make sure all functions are exported like:

```go
var(
    Function1 function1
    Function2 function2
    Functionn functionn
)
```

It is a best practice to combine all related functions in a plugin to simplify the build and deployment of functions.

### Package the source

Build the implemented function as a go plugin and make sure the output so file resides in the plugins/functions folder.

```bash
go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/functions/MyFunction.so extensions/functions/my_function.go
```

### Register multiple functions

eKuiper will load plugins in the plugin folders automatically. The auto loaded function plugin assumes there is a function named the same as the plugin name. If multiple functions are exported, users need to explicitly register them to make them available. There are two ways to register the functions.

1. In development environment, we recommend to build plugin .so file directly into the plugin folder so that eKuiper can auto load it. Then call [CLI register functions command](../cli/plugins.md#register-functions) or [REST register functions API](../restapi/plugins.md#register-functions).
2. In production environment, [package the plugin into zip file](../plugins/plugins_tutorial.md#plugin-deployment-1), then call [CLI function plugin create command](../cli/plugins.md#create-a-plugin) or [REST function plugin create API](../restapi/plugins.md#create-a-plugin) with functions list specified.

## Usage

The customized function can be directly used in the SQL of a rule if it follows the below convention.

If you have developed a function implementation MyFunction, you should have:
1. In the plugin file, symbol MyFunction is exported.
2. The compiled MyFunction.so file is located inside _plugins/functions_

To use it, just call it in the SQL inside a rule definition:
```json
{
  "id": "rule1",
  "sql": "SELECT myFunction(name) from demo",
  "actions": [
    {
      "log": {
      }
    }
  ]
}
```
