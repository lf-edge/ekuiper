# 自定义函数脚本管理

命令行工具允许您管理 UDF，也称为脚本，例如创建、显示、删除、描述脚本。目前，只支持JavaScript函数。

## 注册脚本

该命令用于创建 JavaScript 函数。函数的定义是用 JSON 格式指定的

```shell
create script $script_json
```

示例：

```shell
# bin/kuiper create script "{\"id\": \"area\",\"description\": \"计算面积\",\"script\": \"function area(x, y) { return x * y; }\",\"isAgg\": false}"
```

此命令创建了一个名为 area 的 JavaScript 函数。JSON 对象包含以下字段：

- id：函数的唯一名称。此名称也必须在脚本字段中定义为函数。
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

## 显示所有脚本

该命令用于描述服务器中定义的所有JavaScript函数。

```shell
# bin/kuiper show scripts
```

响应将是函数名称的列表。例如：

```json
["area"]
```

## 描述脚本

该命令打印JavaScript函数的详细定义。

```shell
describe script $script_name
```

示例：

```shell
# bin/kuiper describe script area
{
   "id": "area",
   "description": "计算面积",
   "script": "function area(x, y) { return x * y; }",
   "isAgg": false
}
```

## 删除脚本

该命令删除 JavaScript 函数。

```shell
drop service $script_name
```

示例：

```shell
# bin/kuiper drop script area
```
