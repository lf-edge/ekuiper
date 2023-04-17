# Command device with EdgeX eKuiper rules engine

## Overview

This document describes how to actuate a device with rules trigger by the eKuiper rules engine. To make the example
simple, the virtual device [device-virtual](https://github.com/edgexfoundry/device-virtual-go) is used as the actuated
device. The eKuiper rules engine analyzes the data sent from device-virtual services, and then sends a command to
virtual device based a rule firing in eKuiper based on that analysis. It should be noted that an application service is
used to route core data through the rules engine.

### Use Case Scenarios

Rules will be created in eKuiper to watch for two circumstances:

1. monitor for events coming from the `Random-UnsignedInteger-Device` device (one of the default virtual device managed
   devices), and if a `uint8` reading value is found larger than `20` in the event, then send a command
   to `Random-Boolean-Device` device to start generating random numbers (specifically - set random generation bool to
   true).
2. monitor for events coming from the `Random-Integer-Device` device (another of the default virtual device managed
   devices), and if the average for `int8` reading values (within 20 seconds) is larger than 0, then send a command
   to `Random-Boolean-Device` device to stop generating random numbers (specifically - set random generation bool to
   false).

These use case scenarios do not have any real business meaning, but easily demonstrate the features of EdgeX automatic
actuation accomplished via the eKuiper rule engine.

### Prerequisite Knowledge

This document will not cover basic operations of EdgeX or LF Edge eKuiper. Readers should have basic knowledge of:

- Get and start EdgeX. Refer to [Quick Start](https://docs.edgexfoundry.org/2.0/getting-started/quick-start/) for how to
  get and start EdgeX with the virtual device service.
- Run the eKuiper Rules Engine. Refer
  to [EdgeX eKuiper Rule Engine Tutorial](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/edgex/edgex_rule_engine_tutorial.md)
  to understand the basics of eKuiper and EdgeX.

## Start eKuiper and Create an EdgeX Stream

Make sure you read
the [EdgeX eKuiper Rule Engine Tutorial](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/edgex/edgex_rule_engine_tutorial.md)
and successfully run eKuiper with EdgeX.

First create a stream that can consume streaming data from the EdgeX application service (rules engine profile). This
step is not required if you already finished
the [EdgeX eKuiper Rule Engine Tutorial](https://github.com/lf-edge/ekuiper/blob/master/docs/en_US/edgex/edgex_rule_engine_tutorial.md)
.

``` bash
curl -X POST \
  http://$ekuiper_docker:59720/streams \
  -H 'Content-Type: application/json' \
  -d '{"sql": "create stream demo() WITH (FORMAT=\"JSON\", TYPE=\"edgex\")"}'
```

## Get and Test the Command URL

Since both use case scenario rules will send commands to the `Random-Boolean-Device` virtual device, use the curl
request below to get a list of available commands for this device.

``` bash
curl http://127.0.0.1:59882/api/v2/device/name/Random-Boolean-Device | jq
```

It should print results like those below.

``` json
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

From this output, look for the URL associated to the `PUT` command (the first URL listed). This is the command eKuiper
will use to call on the device. There are two parameters for this command:

- `Bool`: Set the returned value when other services want to get device data. The parameter will be used only
  when `EnableRandomization_Bool` is set to false.
- `EnableRandomization_Bool`: Enable/disable the randomization generation of bool values. If this value is set to true,
  then the 1st parameter will be ignored.

You can test calling this command with its parameters using curl as shown below.

``` bash
curl -X PUT \
  http://edgex-core-command:59882/api/v2/device/name/Random-Boolean-Device/WriteBoolValue \
  -H 'Content-Type: application/json' \
  -d '{"Bool":"true", "EnableRandomization_Bool": "true"}'
```

## Create rules

Now that you have EdgeX and eKuiper running, the EdgeX stream defined, and you know the command to
actuate `Random-Boolean-Device`, it is time to build the eKuiper rules.

### The first rule

Again, the 1st rule is to monitor for events coming from the `Random-UnsignedInteger-Device` device (one of the default
virtual device managed devices), and if a `uint8` reading value is found larger than `20` in the event, then send the
command to `Random-Boolean-Device` device to start generating random numbers (specifically - set random generation bool
to true). 

#### Option 1: Use Rest API

Given the URL and parameters to the command, below is the curl command to declare the first rule in eKuiper.

``` bash
curl -X POST \
  http://$ekuiper_server:59720/rules \
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

#### Option 2: Use Messaging

See [core-command](https://docs.edgexfoundry.org/3.0/microservices/core/command/Ch-Command/#commands-via-messaging) for details. Take the first rule as an example to describe how to configure it:

1. Set the MESSAGEQUEUE_EXTERNAL_ENABLED environment variable to true to enable the external messagebus of core-command.
Set the MESSAGEQUEUE_EXTERNAL_URL environment variable to the address and port number of the external messagebus.
2. Create the rule using the following configuration:
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
The payload is the base64-encoding json struct:
```shell
{"Bool":"true", "EnableRandomization_Bool": "true"}
```
3. Receive command response message from external MQTT broker on topic ```edgex/command/response/#```
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

### The second rule

The 2nd rule is to monitor for events coming from the `Random-Integer-Device` device (another of the default virtual
device managed devices), and if the average for `int8` reading values (within 20 seconds) is larger than 0, then send a
command to `Random-Boolean-Device` device to stop generating random numbers (specifically - set random generation bool
to false). 

#### Option 1: Use Rest API

Here is the curl request to setup the second rule in eKuiper. The same command URL is used as the same device
action (`Random-Boolean-Device's PUT bool command`) is being actuated, but with different parameters.

``` bash
curl -X POST \
  http://$ekuiper_server:59720/rules \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "rule2",
  "sql": "SELECT avg(int8) AS avg_int8 FROM demo WHERE int8 != nil GROUP BY TUMBLINGWINDOW(ss, 20) HAVING avg(int8) > 0",
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

#### Option 2: Use Messaging
The procedure is the same as the previous step. Use the following configuration to create a rule:
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

## Watch the eKuiper Logs

Both rules are now created in eKuiper. eKuiper is busy analyzing the event data coming for the virtual devices looking
for readings that match the rules you created. You can watch the edgex-kuiper container logs for the rule triggering and
command execution.

``` bash
docker logs edgex-kuiper
```

## Explore the Results

You can also explore the eKuiper analysis that caused the commands to be sent to the service. To see the the data from
the analysis, use the SQL below to query eKuiper filtering data.

``` sql
SELECT int8, "true" AS randomization FROM demo WHERE uint8 > 20
```

The output of the SQL should look similar to the results below.

``` json
[{"int8":-75, "randomization":"true"}]
```

Let's suppose a service need following data format, while `value` field is read from field `int8`, and `EnableRandomization_Bool` is read from field `randomization`. 

```shell
curl -X PUT \
  http://edgex-core-command:59882/api/v2/device/name/${deviceName}/command \
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