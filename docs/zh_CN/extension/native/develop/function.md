# 函数扩展

在 eKuiper SQL 语法中，向服务器提供了[许多内置函数](../../../sqls/built-in_functions.md)，用于各种可重用的业务逻辑。 但是，用户仍然可能需要其他未被内置插件覆盖的可重用的业务逻辑。 提供函数扩展是为了自定义函数。

## 开发

### 开发一个定制函数

为 eKuiper 开发函数的过程，就是实现 [api.Function](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) 接口并将其导出为 golang 插件。

在开始开发之前，您必须为 [golang 插件设置环境](../../overview.md#setup-the-plugin-developing-environment)。

为了开发函数，首先要实现 _Validate_ 方法。 在 SQL 验证期间将调用此方法。 在此方法中，将传递 [xsql.Expr](https://github.com/lf-edge/ekuiper/blob/master/pkg/ast/expr.go) 的切片作为参数，该参数包含运行时该函数的参数。 开发人员可以对其进行验证，以检查参数计数和类型等。如果验证成功，则返回 nil。 否则，返回一个错误对象。

```go
//The argument is a list of xsql.Expr
Validate(args []interface{}) error
```
函数有2种类型：聚合函数和通用函数。 对于聚合函数，如果参数为列，则接收的值将始终是组中列值的一部分。 扩展函数必须通过实施 _IsAggregate_ 方法来区分函数类型。

```go
//If this function is an aggregate function. Each parameter of an aggregate function will be a slice
IsAggregate() bool
```

函数的主任务是实现 _exec_ 方法。 该方法将用于计算 SQL 中函数的结果。 参数是函数参数值的一部分。 您可以使用它们进行计算。 如果计算成功，则返回结果并返回 true； 否则，返回 nil 和 false。

```go
//执行函数，如果执行成功,返回结果，如果执行失败，返回错误和 false。
Exec(args []interface{}) (interface{}, bool)
```

由于该函数本身是一个插件，因此必须位于 main 程序包中。 给定的函数结构名称为 myFunction。 在文件的最后，必须将源文件作为符号导出，如下所示。 有[2种类型的导出符号被支持](../../overview.md#plugin-development)。 对于函数扩展，如果没有内部状态，建议导出单例实例。

```go
var MyFunction myFunction
```

[Echo Function](https://github.com/lf-edge/ekuiper/blob/master/extensions/functions/echo/echo.go) 是一个很好的示例。

### 导出多个函数

开发者可在一个函数插件中导出多个函数。每个函数均需实现 [api.Function](https://github.com/lf-edge/ekuiper/blob/master/pkg/api/stream.go) 接口，正如 [开发一个定制函数](#develop-a-customized-function) 所描述的那样。需要确保所有函数都导出了，如下所示：

```go
var(
    Function1 function1
    Function2 function2
    Functionn functionn
)
```

同一类的函数可以在一个插件里开发和导出以减少构建和部署开销。

### 源文件打包
将实现的函数构建为 go 插件，并确保输出 so 文件位于 plugins/functions 文件夹中。

```bash
go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/functions/MyFunction.so extensions/functions/my_function.go
```

### 注册多个函数

eKuiper 启动时会自动载入插件目录里已编译好的插件。自动载入的函数插件假设插件里仅导出一个同名的函数。如果插件导出多个函数，则需要显示运行一次注册操作。有两种方法可以注册函数：

1. 在开发环境中，建议直接构建插件 .so 文件到插件目录中以便 eKuiper 自动载入。构建完成后，运行 [CLI 注册函数命令](../../../operation/cli/plugins.md#register-functions) or [REST 注册函数 API](../../../operation/restapi/plugins.md#register-functions) 进行注册。
2. 在生产环境中，[打包插件到 zip 压缩包](plugins_tutorial.md#plugin-deployment-1)，然后运行 [CLI 创建函数插件命令](../../../operation/cli/plugins.md#create-a-plugin) 或者 [REST 创建函数 API](../../../operation/restapi/plugins.md#create-a-plugin) 并设置 functions 参数以指定导出函数名。

### 使用

如果自定义函数遵循以下约定，则可以直接在规则的 SQL 中使用。

如果已经开发了函数实现 MyFunction，则应该具有：

1. 在插件文件中，将导出符号 MyFunction。
2. 编译的 MyFunction.so 文件位于 _plugins/functions_ 内部

要使用它，只需在规则定义中的 SQL 中调用它：
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
