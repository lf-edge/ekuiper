eKuiper REST api 允许您管理外部服务，例如注册、删除和列出服务，列出外部函数等。

## 注册外部服务

该API接受JSON内容以创建新的外部服务。 

```shell
POST http://localhost:9081/services
```
文件在http服务器上时的请求示例：

```json
{
  "name":"random",
  "file":"http://127.0.0.1/services/sample.zip"
}
```

文件在eKuiper所在服务器上时的请求示例：
```json
{
  "name":"random",
  "file":"file:///var/services/sample.zip"
}
```

### 参数

1. name：外部服务的唯一名称，名称必须与 zip 文件里的服务定义 json 文件完全相同。
2. file：外部服务文件的 URL。URL 支持 http 和 https 以及 file 模式。当使用 file 模式时，该文件必须在 eKuiper 服务器所在的机器上。它必须是一个 zip 文件，其中包含：与服务名相同的服务描述 json 文件以及其他任意辅助文件。其中，schema 文件必须在 schema 文件夹下。

### 服务文件格式
名为 sample.zip 的源的示例 zip 文件
1. sample.json
2. schema 目录：内部包含服务所用到的一个或多个 schema 文件。例如，sample.proto。


## 显示外部服务

该 API 用于显示服务器中为定义的所有外部服务。

```shell
GET http://localhost:9081/services
```

响应示例：

```json
["sample","sample2"]
```

## 描述外部服务

该 API 用于打印外部服务的详细定义。

```shell
GET http://localhost:9081/services/{name}
```

路径参数 `name` 是外部服务的名称。

## 删除外部服务

该 API 用于删除外部服务，服务之下定义的所有函数都将被删除。

```shell
DELETE http://localhost:9081/services/{name}
```

## 更新外部服务

该 API 用于更新外部服务，其参数与服务注册相同。

```shell
PUT http://localhost:9081/services/{name}

{
  "name":"random",
  "file":"http://127.0.0.1/services/sample.zip"
}
```

## 显示所有外部函数

每个服务可包含多个函数。该 API 用于展示所有外部函数的可用于 SQL 的函数名称。

```shell
GET http://localhost:9081/services/functions
```

结果样例：

```json
["func1","func2"]
```

### 描述外部函数

该 API 用于展示定义此外部函数的服务名称。

```shell
GET http://localhost:9081/services/functions/{name}
```

结果样例：

```json
{
  "name": "funcName",
  "serviceName": "serviceName"
}
```