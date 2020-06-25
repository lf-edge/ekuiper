# 函数扩展

在Kuiper SQL语法中，向服务器提供了[许多内置函数](../sqls/built-in_functions.md)，用于各种可重用的业务逻辑。 但是，用户仍然可能需要其他未被内置插件覆盖的可重用的业务逻辑。 提供函数扩展是为了自定义功能。

## 开发

### 开发一个定制函数

为Kuiper开发函数的过程，就是实现 [api.Function](../../../xstream/api/stream.go)接口并将其导出为golang插件。

在开始开发之前，您必须 [为golang插件设置环境](overview.md#setup-the-plugin-developing-environment)。

为了开发函数，首先要实现 _Validate_ 方法。 在SQL验证期间将调用此方法。 在此方法中，将传递[xsql.Expr](../../../xsql/ast.go)的切片作为参数，该参数包含运行时该函数的参数。 开发人员可以对其进行验证，以检查参数计数和类型等。如果验证成功，则返回nil。 否则，返回一个错误对象。

```go
//The argument is a list of xsql.Expr
Validate(args []interface{}) error
```
函数有2种类型：聚合函数和通用函数。 对于聚合函数，如果参数为列，则接收的值将始终是组中列值的一部分。 扩展函数必须通过实施_IsAggregate_方法来区分函数类型。

```go
//If this function is an aggregate function. Each parameter of an aggregate function will be a slice
IsAggregate() bool
```

函数的主要任务是实现_exec_方法。 该方法将用于计算SQL中函数的结果。 参数是函数参数值的一部分。 您可以使用它们进行计算。 如果计算成功，则返回结果并返回true； 否则，返回nil和false。

```go
//Execute the function, return the result and if execution is successful.If execution fails, return the error and false. 
Exec(args []interface{}) (interface{}, bool)
```

由于该函数本身是一个插件，因此必须位于主程序包中。 给定的函数结构名称为myFunction。 在文件的最后，必须将源文件作为符号导出，如下所示。 有[2种类型的导出符号被支持](overview.md#plugin-development)。 对于函数扩展，如果没有内部状态，建议导出单例实例。

```go
var MyFunction myFunction
```

[Echo Function](../../../plugins/functions/echo.go) 是一个很好的示例。

### 源文件打包
将实现的函数构建为go插件，并确保输出so文件位于plugins/functions文件夹中。

```bash
go build --buildmode=plugin -o plugins/functions/MyFunction.so plugins/functions/my_function.go
```

### 使用

如果自定义函数遵循以下约定，则可以直接在规则的SQL中使用。

如果已经开发了函数实现MyFunction，则应该具有：

1. 在插件文件中，将导出符号MyFunction。
2. 编译的MyFunction.so文件位于_plugins/functions_内部

要使用它，只需在规则定义中的SQL中调用它：
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
