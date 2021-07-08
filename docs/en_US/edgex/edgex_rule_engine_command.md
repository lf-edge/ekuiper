# Control device with EdgeX eKuiper rules engine

## Overview

This document describes how to use eKuiper rule engine to control devices with analysis result. To make the tutorial simple,  the doc uses [device-virtual](https://github.com/edgexfoundry/device-virtual-go) sample,  it analyzes the data sent from device-virtual services, and then control the device according to the analysis result produced by eKuiper rule engine.

### Scenarios

In this document, following 2 rules will be created and run.

1. A rule that monitoring `Random-UnsignedInteger-Device` device, and if `uint8` value is  larger than `20`, then send a command to `Random-Boolean-Device` device, and turn on random generation of bool value.
2. A rule that monitoring `Random-Integer-Device` device, and if the average value for `int8` with every 20 seconds is larger than 0, then send a command to `Random-Boolean-Device` device service to turn off random generation of bool value.

The scenario does not have any real business logics, but simply to demonstrate the feature of EdgeX eKuiper rule engine. You can make a reasonable business rules based on our demo.

## Prerequisite knowledge

This document will not cover basic operations for EdgeX & eKuiper, so readers should have basic knowledge for them:

- Refer to [this link](https://docs.edgexfoundry.org/2.0/) for learning basic knowledge of EdgeX, and it would be better to finish [Quick Start](https://docs.edgexfoundry.org/2.0/getting-started/quick-start/).
- Refer to [EdgeX eKuiper Rule Engine Tutorial](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/edgex/edgex_rule_engine_tutorial.md):  You'd better go through this quick tutorial,  and get to start trying out the rules engine in the EdgeX. 
- [Go template](https://golang.org/pkg/text/template/): eKuiper uses Go template for extracting data from analysis result. Knowledge of Go template could help you to extract expected data from analysis result.

## Start to use

Make sure you have followed document [EdgeX eKuiper Rule Engine Tutorial](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/edgex/edgex_rule_engine_tutorial.md), and successfully run the tutorial. 

### Create EdgeX stream

You should create a stream that can consume streaming data from EdgeX application service before creating rule. This step is not required if you already finished [EdgeX eKuiper Rule Engine Tutorial](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/edgex/edgex_rule_engine_tutorial.md). 

```shell
curl -X POST \
  http://$kuiper_docker:59720/streams \
  -H 'Content-Type: application/json' \
  -d '{
  "sql": "create stream demo() WITH (FORMAT=\"JSON\", TYPE=\"edgex\")"
}'
```

Since both of rules will send control command to device `Random-UnsignedInteger-Device`, let's get a list of available
commands for this device by running below command,

`http://127.0.0.1:59882/api/v2/device/name/Random-Boolean-Device | jq`, and it prints similar outputs as below.

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

From the output, you can know that there are four commands, and the 1st command is used for update configurations for
the device. There are two parameters for this device,

- `Bool`: Set the returned value when other services want to get device data. The parameter will be used only when `EnableRandomization_Bool` is set to false.
- `EnableRandomization_Bool`: Enable randomization generation of bool value or not. If this value is set to true, then the 1st parameter will be ignored.

So a sample control command would be similar as following.

```shell
curl -X PUT \
  http://edgex-core-command:59882/api/v2/device/name/Random-Boolean-Device/WriteBoolValue \
  -H 'Content-Type: application/json' \
  -d '{"Bool":"true", "EnableRandomization_Bool": "true"}'
```

### Create rules

#### The first rule

The 1st is a rule that monitoring `Random-UnsignedInteger-Device` device, and if `uint8` value is  larger than `20`, then send a command to `Random-Boolean-Device` device, and turn on random generation of bool value.  Below is the rule definition, please notice that,

- The action will be triggered when uint8 value is larger than 20. Since the uint8 value is not used for sending control command to `Random-Boolean-Device`,  the `uint8` value is not used in the `dataTemplate` property of `rest` action.

```shell
curl -X POST \
  http://$kuiper_server:59720/rules \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "rule1",
  "sql": "SELECT uint8 FROM demo WHERE uint8 > 20",
  "actions": [
    {
      "rest": {
        "url": "http://edgex-core-command:59882/api/v2/device/name/Random-Boolean-Device/WriteBoolValue",
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

#### The 2nd rule

The 2nd rule is monitoring `Random-Integer-Device` device, and if the average value for `int8` with every 20 seconds is larger than 0, then send a command to `Random-Boolean-Device` device service to turn off random generation of bool value.

- The average value for uint8 is calculated every 20 seconds, and if the average value is larger than 0, then send a control command to `Random-Boolean-Device` service.

```shell
curl -X POST \
  http://$kuiper_server:59720/rules \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "rule2",
  "sql": "SELECT avg(int8) AS avg_int8 FROM demo WHERE int8 != nil GROUP BY TUMBLINGWINDOW(ss, 20) HAVING avg(int8) > 0",
  "actions": [
    {
      "rest": {
        "url": "http://edgex-core-command:59882/api/v2/device/name/Random-Boolean-Device/WriteBoolValue",
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

Now both of rules are created, and you can take a look at logs of edgex-kuiper for the rule execution result.

```shell
# docker logs edgex-kuiper
```

## Extract data from analysis result?

It is probably that the analysis result need to be sent to command rest service as well, how to extract the data from analysis result? For example, below SQL is used for filtering data.

```sql
SELECT int8, "true" AS randomization FROM demo WHERE uint8 > 20
```

The output of the SQL is probably similar as below,

```json
[{"int8":-75, "randomization":"true"}]
```

Let's suppose a service need following data format, while `value` field is read from field `int8`, and `EnableRandomization_Bool` is read from field `randomization`. 

```shell
curl -X PUT \
  http://edgex-core-command:59882/api/v1/device/${deviceId}/command/xyz \
  -H 'Content-Type: application/json' \
  -d '{"value":-75, "EnableRandomization_Bool": "true"}'
```

eKuiper uses [Go template](https://golang.org/pkg/text/template/) to extract data from analysis result, and the `dataTemplate` should be similar as following.

```
"dataTemplate": "{\"value\": {{.int8}}, \"EnableRandomization_Bool\": \"{{.randomization}}\"}"
```

In some cases, you probably need to iterate over returned array values, or set different values with if conditions, then refer to [this link](https://golang.org/pkg/text/template/#hdr-Actions) for writing more complex data template expressions.

## Extended readings

 If you want to explore more features of eKuiper, please refer to below resources.

- [eKuiper Github code repository](https://github.com/lf-edge/ekuiper/)
- [eKuiper reference guide](https://github.com/lf-edge/ekuiper/blob/edgex/docs/en_US/reference.md)

