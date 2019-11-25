# Function Extension

In the Kuiper SQL syntax, [many built-in functions](../sqls/built-in_functions.md) are provided to server for various reusable business logic. However, the users still likely need various reusable business logic which are not covered by the built ins. The function extension is presented to customized the functions.

## Developing

### Develop a customized function

To develop a function for Kuiper is to implement [api.Function](../../../xstream/api/stream.go) interface and export it as a golang plugin.

Before starting the development, you must [setup the environment for golang plugin](overview.md#setup-the-plugin-developing-environment). 

To develop a function, the _Validate_ method is firstly to be implemented. This method will be called during SQL validation. In this method, a slice of [xsql.Expr](../../../xsql/ast.go) is passed as the parameter that contains the arguments for this function in the runtime. The developer can do a validation against it to check the argument count and type etc. If validation is successful, return nil. Otherwise, return an error object.

```go
//The argument is a list of xsql.Expr
Validate(args []interface{}) error
```
The main task for a Function is to implement _exec_ method. The method will be leverage to calculate the result of the function in the SQL. The argument is a slice of the values for the function parameters. You can use them to do the calculation. If the calculation is successful, return the result and true; otherwise, return nil and false. 

```go
//Execute the function, return the result and if execution is successful.If execution fails, return the error and false. 
Exec(args []interface{}) (interface{}, bool)
```  

As the function itself is a plugin, it must be in the main package. Given the function struct name is myFunction. At last of the file, the source must be exported as a symbol as below.

```go
var MyFunction myFunction
```

The [Echo Function](../../../plugins/functions/echo.go) is a good example.

### Package the source
Build the implemented function as a go plugin and make sure the output so file resides in the plugins/functions folder.

```bash
go build --buildmode=plugin -o plugins/functions/MyFunction.so plugins/functions/my_function.go
```

### Usage

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
