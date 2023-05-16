# Services management

The eKuiper command line tools allows you to manage services, such as create, show, drop, describe services.

## Register service

The command is used for creating a service. The service's definition is specified with JSON format

```shell
create service $service_name $service_json
```

Service package file should be prepared at first and put at a place that eKuiper can access.

Example：

```shell
# bin/kuiper create service sample '{"name": "sample","file": "file:///tmp/sample.zip"}'
```

This command creates a service named sample whose content is provided by `file` field in the json. 


## Show services and service_funcs

The command is used for describing all services and service_funcs defined in the server.

```shell
# bin/kuiper show services
```

```shell
# bin/kuiper show service_funcs
```

## Describe a service

The command prints the detailed definition of a service.

```shell
describe service $service_name
```

Example：

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


## Describe a service function

The command prints the detailed information of a service function.

```shell
describe service_func $service_name
```

Example：

```shell
# bin/kuiper describe service_func label
{
  "ServiceName": "sample",
  "InterfaceName": "trueno",
  "MethodName": "label"
}
```


## Drop a service

The command drops the service.

```shell
drop service $service_name
```

Example：

```shell
# bin/kuiper drop service sample
```