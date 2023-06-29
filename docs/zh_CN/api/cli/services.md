# 服务管理

eKuiper 命令行工具允许您管理服务，例如创建、显示、删除、描述服务。

## 注册服务

该命令用于创建服务。服务的定义以 JSON 格式指定

```shell
create service $service_name $service_json
```

首先要准备打包好的服务描述信息，放在 eKuiper 可以访问的地方

示例：

```shell
# bin/kuiper create service sample '{"name": "sample","file": "file:///tmp/sample.zip"}'
```

这个命令创建了一个名为 sample 的服务，该服务的具体描述信息放在了 file 路径指定的地方。

## 查看服务和服务函数

此命令查看 eKuiper 系统中注册的所有服务

```shell
# bin/kuiper show services
```

此命令查看 eKuiper 系统中注册的所有服务函数

```shell
# bin/kuiper show service_funcs
```

## 查看服务的详细信息

此命令可以查看服务的详细信息

```shell
describe service $service_name
```

示例：

```shell
# bin/kuiper describe service sample
{
  "About": {
    "author": {
      "name": "EMQ",
      "email": "contact@emqx.io",
      "company": "EMQ Technologies Co., Ltd",
      "website": "https://www.emqx.io"
    },
    "helpUrl": {
      "en_US": "https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/plugins/functions/functions.md",
      "zh_CN": "https://github.com/lf-edge/ekuiper/blob/master/docs/zh_CN/plugins/functions/functions.md"
    },
    "description": {
      "en_US": "Sample external services for test only",
      "zh_CN": "示例外部函数配置，仅供测试"
    }
  },
  "Interfaces": {
    "trueno": {
      "Desc": null,
      "Addr": "tcp://localhost:50051",
      "Protocol": "grpc",
      "Schema": {
        "SchemaType": "protobuf",
        "SchemaFile": "sample.proto"
      },
      "Functions": [
        "label"
      ],
      "Options": null
    }
  }
}

```

## 描述服务函数详细信息

此命令可以列出服务函数详细信息

```shell
describe service_func $service_name
```

示例：

```shell
# bin/kuiper describe service_func label
{
  "ServiceName": "sample",
  "InterfaceName": "trueno",
  "MethodName": "label"
}
```

## 删除服务

此命令可以删除服务

```shell
drop service $service_name
```

示例：

```shell
# bin/kuiper drop service sample
```
