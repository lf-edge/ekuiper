# 用户定义函数（UDF）管理 API

除了在[插件](./plugins.md)中定义函数，我们还支持用户单独定义函数（UDF）。目前，我们只支持 JavaScript UDF。用户可以通过 REST API 或者 [CLI](../cli/scripts.md) 管理 JavaScript 函数。您可以创建、列出、描述、删除和更新函数。

## 创建 UDF

使用此端点创建新函数。

```shell
POST http://localhost:9081/udf/javascript
```

请求体应为一个 JSON 对象，包含以下字段：

- id：函数的唯一名称。此名称也必须在 script 字段中定义为函数。
- description：函数的简短描述。
- script：JavaScript 中的函数实现。
- isAgg：一个布尔值，表示函数是否为聚合函数。

以下是一个示例：

```json
{
  "id": "area",
  "description": "计算面积",
  "script": "function area(x, y) { return x * y; }",
  "isAgg": false
}
```

## 列出 UDF

使用此端点显示服务器中定义的所有 JavaScript 函数。

```shell
GET http://localhost:9081/udf/javascript
```

响应将是函数名称的列表。例如：

```json
["area"]
```

## 描述 UDF

使用此端点获取函数的详细定义。

```shell
GET http://localhost:9081/udf/javascript/{id}
```

将 {id} 替换为您要描述的函数的名称。响应将是一个 JSON 对象，包含函数的详细信息。例如：

```json
{
  "id": "area",
  "description": "计算面积",
  "script": "function area(x, y) { return x * y; }",
  "isAgg": false
}
```

## 删除 UDF

使用此端点删除函数。

```shell
DELETE http://localhost:9081/udf/javascript/{id}
```

将 {id} 替换为您要删除的函数的名称。请注意，您需要手动停止或删除使用 UDF 的任何规则，然后再删除它。正在运行的规则不会受到 UDF 删除的影响。

## 更新 UDF

JavaScript UDF 可以更新和热重载。请注意，必须重新启动正在运行的规则才能加载更新的函数。

```shell
PUT http://localhost:9081/udf/javascript/{id}
```

将 {id} 替换为您要更新的函数的名称。请求体应与创建 UDF 时相同。如果 id 的函数不存在，将创建它。否则，将更新它。
