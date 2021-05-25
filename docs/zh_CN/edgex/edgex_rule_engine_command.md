# 使用 eKuiper 规则引擎控制设备

## 概述

该文章描述了如何在 EdgeX 中使用 eKuiper 规则引擎，根据分析结果来实现对设备的控制。为了便于理解，该文章使用 [device-virtual](https://github.com/edgexfoundry/device-virtual-go)示例，它对device-virtual服务发送的数据进行分析，然后根据由Kuiper规则引擎生成的分析结果来控制设备 。

### 场景

在本文中，将创建并运行以下两条规则。

1. 监视`Random-UnsignedInteger-Device`设备的规则，如果`uint8`值大于 `20`，则向`Random-Boolean-Device`设备发送命令，并开启布尔值的随机生成 。
2. 监视`Random-Integer-Device`设备的规则，如果每20秒 `int8`的平均值大于0，则向`Random-Boolean-Device` 设备服务发送命令以关闭 布尔值的随机生成。

该场景不含任何真实的业务逻辑，而只是为了演示eKuiper规则引擎的功能。 您可以根据我们的演示制定合理的业务规则。

## 预备知识

本文档将不涉及EdgeX和eKuiper的基本操作，因此读者应具有以下基本知识：

- 请通过[此链接](https://docs.edgexfoundry.org/2.0/) 以了解EdgeX的基础知识，最好完成[快速入门](https://docs.edgexfoundry.org/1.2/getting-started/quick-start/)。
- 请阅读[EdgeX eKuiper规则引擎入门教程](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/edgex/edgex_rule_engine_tutorial.md)：您最好阅读此入门教程，并开始在EdgeX中试用规则引擎。
- [Go模板](https://golang.org/pkg/text/template/)：eKuiper使用Go模板从分析结果中提取数据。 了解Go模板可以帮助您从分析结果中提取所需的数据。

## 开始使用

请务必遵循文档 [EdgeX eKuiper规则引擎入门教程](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/edgex/edgex_rule_engine_tutorial.md)，确保教程能够成功运行。

### 创建EdgeX流

在创建规则之前，应创建一个流，该流可以使用来自EdgeX应用程序服务的流数据。 如果您已经完成 [EdgeX eKuiper规则引擎入门教程](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/edgex/edgex_rule_engine_tutorial.md)，则不需要此步骤。

```shell
curl -X POST \
  http://$ekuiper_docker:48075/streams \
  -H 'Content-Type: application/json' \
  -d '{
  "sql": "create stream demo() WITH (FORMAT=\"JSON\", TYPE=\"edgex\")"
}'
```

由于这两个规则都会向设备`Random-UnsignedInteger-Device`发送控制命令，通过运行命令`curl http://localhost:48082/api/v1/device/name/Random-Boolean-Device | jq`可以获取该设备的可用命令列表。它将打印类似的输出，如下所示。

```json
{
  "id": "9b051411-ca20-4556-bd3e-7f52475764ff",
  "name": "Random-Boolean-Device",
  "adminState": "UNLOCKED",
  "operatingState": "ENABLED",
  "labels": [
    "device-virtual-example"
  ],
  "commands": [
    {
      "created": 1589052044139,
      "modified": 1589052044139,
      "id": "28d88bb3-e280-46f7-949f-37cc411757f5",
      "name": "Bool",
      "get": {
        "path": "/api/v1/device/{deviceId}/Bool",
        "responses": [
          {
            "code": "200",
            "expectedValues": [
              "Bool"
            ]
          },
          {
            "code": "503",
            "description": "service unavailable"
          }
        ],
        "url": "http://edgex-core-command:48082/api/v1/device/bcd18c02-b187-4f29-8265-8312dc5d794d/command/d6d3007d-c4ce-472f-a117-820b5410e498"
      },
      "put": {
        "path": "/api/v1/device/{deviceId}/Bool",
        "responses": [
          {
            "code": "200"
          },
          {
            "code": "503",
            "description": "service unavailable"
          }
        ],
        "url": "http://edgex-core-command:48082/api/v1/device/bcd18c02-b187-4f29-8265-8312dc5d794d/command/d6d3007d-c4ce-472f-a117-820b5410e498",
        "parameterNames": [
          "Bool",
          "EnableRandomization_Bool"
        ]
      }
    }
  ]
}
```

从输出中，您能看出有两个命令，第二个命令用于更新设备的配置。 此设备有两个参数：

- `Bool`：当其他服务想要获取设备数据时，设置返回值。 仅当`EnableRandomization_Bool`设置为false时，才使用该参数。
- `EnableRandomization_Bool`：是否启用`Bool`的随机生成。 如果将此值设置为true，则将忽略第一个参数。

因此，示例控制命令将类似于如下命令：

```shell
curl -X PUT \
  http://edgex-core-command:48082/api/v1/device/c1459444-79bd-46c8-8b37-d6e1418f2a3a/command/fe202437-236d-41c5-845e-3e6013b928cd \
  -H 'Content-Type: application/json' \
  -d '{"Bool":"true", "EnableRandomization_Bool": "true"}'
```

### 创建规则

#### 第一条规则

第一条规则是监视`Random-UnsignedInteger-Device`设备的规则，如果`uint8`值大于“ 20”，则向`Random-Boolean-Device`设备发送命令，并开启布尔值的随机生成 。 以下是规则定义，请注意：

- 当uint8的值大于20时将触发该动作。由于uint8的值不用于向`Random-Boolean-Device`发送控制命令，因此在`rest`操作的`dataTemplate`属性中不使用`uint8`值。

```shell
curl -X POST \
  http://$eKuiper_server:48075/rules \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "rule1",
  "sql": "SELECT uint8 FROM demo WHERE uint8 > 20",
  "actions": [
    {
      "rest": {
        "url": "http://edgex-core-command:48082/api/v1/device/bcd18c02-b187-4f29-8265-8312dc5d794d/command/d6d3007d-c4ce-472f-a117-820b5410e498",
        "method": "put",
        "retryInterval": -1,
        "dataTemplate": "{\"Bool\":\"true\", \"EnableRandomization_Bool\": \"true\"}",
        "sendSingle": true
      }
    },
    {
      "log":{}
    }
  ]
}'
```

#### 第二条规则

第二条规则监视`Random-Integer-Device`设备，如果每20秒 `int8`的平均值大于0，则向`Random-Boolean-Device` 设备服务发送命令以关闭 布尔值的随机生成。

- uint8的平均值每20秒计算一次，如果平均值大于0，则向 `Random-Boolean-Device` 服务发送控制命令。

```shell
curl -X POST \
  http://$eKuiper_server:48075/rules \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "rule2",
  "sql": "SELECT avg(int8) AS avg_int8 FROM demo WHERE int8 != nil GROUP BY  TUMBLINGWINDOW(ss, 20) HAVING avg(int8) > 0",
  "actions": [
    {
      "rest": {
        "url": "http://edgex-core-command:48082/api/v1/device/bcd18c02-b187-4f29-8265-8312dc5d794d/command/d6d3007d-c4ce-472f-a117-820b5410e498",
        "method": "put",
        "retryInterval": -1,
        "dataTemplate": "{\"Bool\":\"false\", \"EnableRandomization_Bool\": \"false\"}",
        "sendSingle": true
      }
    },
    {
      "log":{}
    }
  ]
}'
```

现在创建了两个规则，您可以查看edgex-kuiper的日志以获取规则执行结果。

```shell
# docker logs edgex-kuiper
```

## 如何从分析结果中提取数据？

由于分析结果也需要发送到command rest服务，如何从分析结果中提取数据？通过SQL过滤数据的示例如下所示：

```sql
SELECT int8, "true" AS randomization FROM demo WHERE uint8 > 20
```

SQL的输出内容如下：

```json
[{"int8":-75, "randomization":"true"}]
```

当从字段`int8`读取`value`字段，从字段`randomization`读取`EnableRandomization_Bool`时，假设服务需要以下数据格式：

```shell
curl -X PUT \
  http://edgex-core-command:48082/api/v1/device/${deviceId}/command/xyz \
  -H 'Content-Type: application/json' \
  -d '{"value":-75, "EnableRandomization_Bool": "true"}'
```

eKuiper使用[Go模板](https://golang.org/pkg/text/template/) 从分析结果中提取数据，并且`dataTemplate` 内容如下：

```
"dataTemplate": "{\"value\": {{.int8}}, \"EnableRandomization_Bool\": \"{{.randomization}}\"}"
```

在某些情况下，您可能需要迭代返回的数组值，或使用if条件设置不同的值，然后参考[此链接](https://golang.org/pkg/text/template/#hdr-Actions)写入更复杂的数据模板表达式。

## 补充阅读材料

如果您想了解LF Edge eKuiper的更多特性，请阅读下面的参考资料：

- [eKuiper Github 代码库](https://github.com/lf-edge/ekuiper/)
- [eKuiper 参考指南](https://github.com/lf-edge/ekuiper/blob/edgex/docs/en_US/reference.md)

