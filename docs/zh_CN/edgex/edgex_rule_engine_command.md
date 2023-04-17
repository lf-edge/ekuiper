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
  http://$ekuiper_docker:59720/streams \
  -H 'Content-Type: application/json' \
  -d '{
  "sql": "create stream demo() WITH (FORMAT=\"JSON\", TYPE=\"edgex\")"
}'
```

由于这两个规则都会向设备`Random-UnsignedInteger-Device`
发送控制命令，通过运行命令`curl http://127.0.0.1:59882/api/v2/device/name/Random-Boolean-Device | jq`可以获取该设备的可用命令列表。它将打印类似的输出，如下所示。

```json
{
  "apiVersion": "v2",
  "statusCode": 200,
  "deviceCoreCommand": {
    "deviceName": "Random-Boolean-Device",
    "profileName": "Random-Boolean-Device",
    "coreCommands": [
      {
        "name": "WriteBoolValue",
        "set": true,
        "path": "/api/v2/device/name/Random-Boolean-Device/WriteBoolValue",
        "url": "http://edgex-core-command:59882",
        "parameters": [
          {
            "resourceName": "Bool",
            "valueType": "Bool"
          },
          {
            "resourceName": "EnableRandomization_Bool",
            "valueType": "Bool"
          }
        ]
      },
      {
        "name": "WriteBoolArrayValue",
        "set": true,
        "path": "/api/v2/device/name/Random-Boolean-Device/WriteBoolArrayValue",
        "url": "http://edgex-core-command:59882",
        "parameters": [
          {
            "resourceName": "BoolArray",
            "valueType": "BoolArray"
          },
          {
            "resourceName": "EnableRandomization_BoolArray",
            "valueType": "Bool"
          }
        ]
      },
      {
        "name": "Bool",
        "get": true,
        "set": true,
        "path": "/api/v2/device/name/Random-Boolean-Device/Bool",
        "url": "http://edgex-core-command:59882",
        "parameters": [
          {
            "resourceName": "Bool",
            "valueType": "Bool"
          }
        ]
      },
      {
        "name": "BoolArray",
        "get": true,
        "set": true,
        "path": "/api/v2/device/name/Random-Boolean-Device/BoolArray",
        "url": "http://edgex-core-command:59882",
        "parameters": [
          {
            "resourceName": "BoolArray",
            "valueType": "BoolArray"
          }
        ]
      }
    ]
  }
}
```

从输出中，您能看出有两个命令，第二个命令用于更新设备的配置。 此设备有两个参数：

- `Bool`：当其他服务想要获取设备数据时，设置返回值。 仅当`EnableRandomization_Bool`设置为false时，才使用该参数。
- `EnableRandomization_Bool`：是否启用`Bool`的随机生成。 如果将此值设置为true，则将忽略第一个参数。

因此，示例控制命令将类似于如下命令：

```shell
curl -X PUT \
  http://edgex-core-command:59882/api/v2/device/name/Random-Boolean-Device/WriteBoolValue \
  -H 'Content-Type: application/json' \
  -d '{"Bool":"true", "EnableRandomization_Bool": "true"}'
```

### 创建规则

#### 第一条规则

第一条规则是监视`Random-UnsignedInteger-Device`设备的规则，如果`uint8`值大于“ 20”，则向`Random-Boolean-Device`设备发送命令，并开启布尔值的随机生成 。 

##### 使用 Rest API

以下是规则定义，请注意：
- 当uint8的值大于20时将触发该动作。由于uint8的值不用于向`Random-Boolean-Device`发送控制命令，因此在`rest`操作的`dataTemplate`属性中不使用`uint8`值。

```shell
curl -X POST \
  http://$eKuiper_server:59720/rules \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "rule1",
  "sql": "SELECT uint8 FROM demo WHERE uint8 > 20",
  "actions": [
    {
      "rest": {
        "url": "http://edgex-core-command:59882/api/v2/device/name/Random-Boolean-Device/WriteBoolValue",
        "method": "put",
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

##### 使用 Messaging

具体信息请详见[core-command](https://docs.edgexfoundry.org/3.0/microservices/core/command/Ch-Command/#commands-via-messaging)。这里以第一条规则为例，简单介绍一下如何配置：

1. 将MESSAGEQUEUE_EXTERNAL_ENABLED环境变量设为true，开启core-command的external messagebus；
将MESSAGEQUEUE_EXTERNAL_URL环境变量设为external messagebus的地址和端口号。
2. 使用如下配置创建规则：
```shell
{
  "sql": "SELECT uint8 FROM demo WHERE uint8 > 20",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://mqtt-server:1883",
        "topic": "edgex/command/request/Random-Boolean-Device/WriteBoolValue/set",
        "dataTemplate": "{\"ApiVersion\": \"v2\", \"contentType\": \"application/json\", \"CorrelationID\": \"14a42ea6-c394-41c3-8bcd-a29b9f5e6840\", \"RequestId\": \"e6e8a2f4-eb14-4649-9e2b-175247911380\", \"Payload\": \"eyJCb29sIjogInRydWUiLCAiRW5hYmxlUmFuZG9taXphdGlvbl9Cb29sIjogInRydWUifQ==\"}"
      }
    },
    {
      "log":{}
    }
  ]
}
```
其中payload是```{"Bool":"true", "EnableRandomization_Bool": "true"}```的base64编码。
3. 发送成功后，可以在```edgex/command/response/#```topic里收到如下response：
```shell
{
  "ReceivedTopic": "edgex/device/command/response/device-virtual/Random-Boolean-Device/WriteBoolValue/set",
  "CorrelationID": "14a42ea6-c394-41c3-8bcd-a29b9f5e6840",
  "ApiVersion": "v2",
  "RequestID": "e6e8a2f4-eb14-4649-9e2b-175247911380",
  "ErrorCode": 0,
  "Payload": null,
  "ContentType": "application/json",
  "QueryParams": {}
}

```

#### 第二条规则

第二条规则监视`Random-Integer-Device`设备，如果每20秒 `int8`的平均值大于0，则向`Random-Boolean-Device` 设备服务发送命令以关闭布尔值的随机生成。

- uint8的平均值每20秒计算一次，如果平均值大于0，则向 `Random-Boolean-Device` 服务发送控制命令。

##### 使用 Rest API

```shell
curl -X POST \
  http://$eKuiper_server:59720/rules \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "rule2",
  "sql": "SELECT avg(int8) AS avg_int8 FROM demo WHERE int8 != nil GROUP BY  TUMBLINGWINDOW(ss, 20) HAVING avg(int8) > 0",
  "actions": [
    {
      "rest": {
        "url": "http://edgex-core-command:59882/api/v2/device/name/Random-Boolean-Device/WriteBoolValue",
        "method": "put",
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

##### 使用 Messaging

具体步骤同上，使用如下配置创建规则：
```shell
{
  "sql": "SELECT avg(int8) AS avg_int8 FROM demo WHERE int8 != nil GROUP BY  TUMBLINGWINDOW(ss, 20) HAVING avg(int8) > 0",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://mqtt-server:1883",
        "topic": "edgex/command/request/Random-Boolean-Device/WriteBoolValue/set",
        "dataTemplate": "{\"ApiVersion\": \"v2\", \"contentType\": \"application/json\", \"CorrelationID\": \"14a42ea6-c394-41c3-8bcd-a29b9f5e6840\", \"RequestId\": \"e6e8a2f4-eb14-4649-9e2b-175247911380\", \"Payload\": \"eyJCb29sIjogImZhbHNlIiwgIkVuYWJsZVJhbmRvbWl6YXRpb25fQm9vbCI6ICJmYWxzZSJ9\"}"
      }
    },
    {
      "log":{}
    }
  ]
}
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
  http://edgex-core-command:59882/api/v2/device/name/${deviceName}/command \
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

