# EdgeX Message Bus action

The action is used for publishing output message into EdgeX message bus.  

**Please notice that, if you're using the ZeorMQ message bus, the action will create a NEW EdgeX message bus (with the address where running eKuiper service), but not by leveraging the original message bus (normally it's the address & port exposed by application service).**

**Also, you need to expose the port number to host server before running the eKuiper server if you want to have the service available to other hosts.**

| Property name      | Optional | Description                                                                                                                                                                                                                                                                                  |
|--------------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| type               | true     | The message bus type, three types of message buses are supported, `zero`, `mqtt` and `redis`, and `redis` is the default value.                                                                                                                                                      |
| protocol           | true     | The protocol. If it's not specified, then use default value `redis`.                                                                                                                                                                                                                       |
| host               | true     | The host of message bus. If not specified, then use default value `localhost`.                                                                                                                                                                                                             |
| port               | true     | The port of message bus. If not specified, then use default value `6379`.                                                                                                                                                                                                                  |
| connectionSelector | true     | reuse the connection to EdgeX message bus. [more info](../sources/edgex.md#connectionselector)                                                                                                                                                                                               |
| topic              | true     | The topic to be published. The topic is static across all messages. To use dynamic topic, leave this empty and specify the topicPrefix property. Only one of the topic and topicPrefix properties can be specified. If both are not specified, then use default topic value `application`. |
| topicPrefix        | true     | The prefix of a dynamic topic to be published. The topic will become a concatenation of `$topicPrefix/$profileName/$deviceName/$sourceName`.                                                                                                                                                 |
| contentType        | true     | The content type of message to be published. If not specified, then use the default value `application/json`.                                                                                                                                                                              |
| messageType        | true     | The EdgeX message model type. To publish the message as an event like EdgeX application service, use `event`. Otherwise, to publish the message as an event request like EdgeX device service or core data service, use `request`. If not specified, then use the default value `event`.   |
| metadata           | true     | The property is a field name that allows user to specify a field name of SQL  select clause,  the field name should use `meta(*) AS xxx`  to select all of EdgeX metadata from message.                                                                                                    |
| profileName        | true     | Allows user to specify the profile name in the event structure that are sent from eKuiper. The profileName in the meta take precedence if specified.                                                                                                                                         |
| deviceName         | true     | Allows user to specify the device name in the event structure that are sent from eKuiper. The deviceName in the meta take precedence if specified.                                                                                                                                           |
| sourceName         | true     | Allows user to specify the source name in the event structure that are sent from eKuiper. The sourceName in the meta take precedence if specified.                                                                                                                                           |
| optional           | true     | If `mqtt` message bus type is specified, then some optional values can be specified. Please refer to below for supported optional supported configurations.                                                                                                                                |

Below optional configurations are supported, please check MQTT specification for the detailed information.

- optional
  - ClientId
  - Username
  - Password
  - Qos
  - KeepAlive
  - Retained
  - ConnectionPayload
  - CertFile
  - KeyFile
  - CertPEMBlock
  - KeyPEMBlock
  - SkipCertVerify

## Send to various targets

By setting the combination of the properties, we can send the result to various EdgeX message bus settings.

### Publish to redis message bus like application service

With the default setting, the EdgeX sink will publish to the default redis message bus as application events. In EdgeX, those messages can be consumed like events emitted by application service. 

```json
{
  "id": "ruleRedisEvent",
  "sql": "SELECT temperature * 3 AS t1, humidity FROM events",
  "actions": [
    {
      "edgex": {
        "protocol": "redis",
        "host": "localhost",
        "port": 6379,
        "topic": "application",
        "profileName": "ekuiperProfile",
        "deviceName": "ekuiper",        
        "contentType": "application/json"
      }
    }
  ]
}
```

### Publish to redis message bus like device service

By changing the `topicPrefix` and `messageType` properties, we can let EdgeX sink simulates a device. The topic name for device in EdgeX is like `edgex/events/device/$profileName/$deviceName/$sourceName` so we set the `topicPrefix` to `edgex/events/device` to make sure the messages are routing to device events. And by specifying the `metadata` property, we can have a dynamic topic to simulate multiple devices. Check the next section [dynamic metadata](#dynamic-metadata) for details.

```json
{
  "id": "ruleRedisDevice",
  "sql": "SELECT temperature * 3 AS t1, humidity FROM events",
  "actions": [
    {
      "edgex": {
        "protocol": "redis",
        "host": "localhost",
        "port": 6379,
        "topicPrefix": "edgex/events/device",
        "messageType": "request",
        "metadata": "metafield_name",
        "contentType": "application/json"
      }
    }
  ]
}
```

## Publish to MQTT message bus

Below is a rule that send analysis result to MQTT message bus, please notice how to specify `ClientId` in `optional` configuration. 

```json
{
  "id": "ruleMqtt",
  "sql": "SELECT meta(*) AS edgex_meta, temperature, humidity, humidity*2 as h1 FROM demo WHERE temperature = 20",
  "actions": [
    {
      "edgex": {
        "protocol": "tcp",
        "host": "127.0.0.1",
        "port": 1883,
        "topic": "result",
        "type": "mqtt",
        "metadata": "edgex_meta",
        "contentType": "application/json",
        "optional": {
        	"ClientId": "edgex_message_bus_001"
        }
      }
    }
  ]
}
```

## Publish to zeromq message bus

Below is a rule that send analysis result to zeromq message bus.

```json
{
  "id": "ruleZmq",
  "sql": "SELECT meta(*) AS edgex_meta, temperature, humidity, humidity*2 as h1 FROM demo WHERE temperature = 20",
  "actions": [
    {
      "edgex": {
        "protocol": "tcp",
        "host": "*",
        "port": 5571,
        "topic": "application",
        "profileName": "myprofile",
        "deviceName": "mydevice",        
        "contentType": "application/json"
      }
    }
  ]
}
```

## Connection reuse publish example

Below is an example for how to use connection reuse feature. We just need remove the connection related parameters and
use the `connectionSelector` to specify the connection to reuse. [more info](../sources/edgex.md#connectionselector)

```json
{
  "id": "ruleRedisDevice",
  "sql": "SELECT temperature, humidity, humidity*2 as h1 FROM demo WHERE temperature = 20",
  "actions": [
    {
      "edgex": {
        "connectionSelector": "edgex.redisMsgBus",
        "topic": "application",
        "profileName": "myprofile",
        "deviceName": "mydevice",        
        "contentType": "application/json"
      }
    }
  ]
}
```


## Dynamic metadata

### Publish result to a new EdgeX message bus without keeping original metadata
In this case, the original metadata value (such as `id, profileName, deviceName, sourceName, origin, tags` in `Events` structure, and `id, profileName, deviceName, origin, valueType` in `Reading` structure will not be kept). eKuiper acts as another EdgeX micro service here, and it has own `device name` and `profile name`. `deviceName` and `profileName` properties are provided, and allows user to specify the device name of eKuiper. The `SourceName` will be default to the `topic` property. Below is one example,

1) Data received from EdgeX message bus `events` topic,
```
{
  "DeviceName": "demo", "Origin": 000, …
  "readings": 
  [
     {"ResourceName": "Temperature", value: "30", "Origin":123 …},
     {"ResourceName": "Humidity", value: "20", "Origin":456 …}
  ]
}
```
2) Use following rule,  and specify `deviceName` with `kuiper` and `profileName` with `kuiperProfile` in `edgex` action.

```json
{
  "id": "rule1",
  "sql": "SELECT temperature * 3 AS t1, humidity FROM events",
  "actions": [
    {
      "edgex": {
        "topic": "application",
        "deviceName": "kuiper",
        "profileName": "kuiperProfile",
        "contentType": "application/json"
      }
    }
  ]
}
```
3) The data sent to EdgeX message bus.
```
{
  "DeviceName": "kuiper", "ProfileName": "kuiperProfile",  "Origin": 0, …
  "readings": 
  [
     {"ResourceName": "t1", value: "90", "Origin": 0 …},
     {"ResourceName": "humidity", value: "20" , "Origin": 0 …}
  ]
}
```
Please notice that, 
- The device name of `Event` structure is changed to `kuiper` and the profile name is changed to `kuiperProfile`.
- All of metadata for `Events and Readings` structure will be updated with new value. `Origin` field is updated to another value generated by eKuiper (here is `0``).

### Publish result to a new EdgeX message bus keeping original metadata

But for some scenarios, you may want to keep some of original metadata. Such as keep the device name as original value that published to eKuiper (`demo` in the sample), and also other metadata of readings arrays. In such case, eKuiper is acting as a filter - to filter NOT concerned messages, but still keep original data.

Below is an example,

1) Data received from EdgeX message bus `events` topic,
```
{
  "DeviceName": "demo", "Origin": 000, …
  "readings": 
  [
     {"ResourceName": "Temperature", value: "30", "Origin":123 …},
     {"ResourceName": "Humidity", value: "20", "Origin":456 …}
  ]
}
```
2) Use following rule,  and specify `metadata` with `edgex_meta`  in `edgex` action.

```json
{
  "id": "rule1",
  "sql": "SELECT meta(*) AS edgex_meta, temperature * 3 AS t1, humidity FROM events WHERE temperature > 30",
  "actions": [
    {
      "edgex": {
        "topic": "application",
        "metadata": "edgex_meta",
        "contentType": "application/json"
      }
    }
  ]
}
```
Please notice that,
- User need to add `meta(*) AS edgex_meta` in the SQL clause, the `meta(*)` returns all of metadata.
- In `edgex` action, value `edgex_meta`  is specified for `metadata` property. This property specifies which field contains metadata of message.

3) The data sent to EdgeX message bus.
```
{
  "DeviceName": "demo", "Origin": 000, …
  "readings": 
  [
     {"ResourceName": "t1", value: "90" , "Origin": 0 …},
     {"ResourceName": "humidity", value: "20", "Origin":456 …}
  ]
}
```
Please notice that,
- The metadata of `Events` structure is still kept, such as `DeviceName` & `Origin`.
- For the reading that can be found in original message, the metadata will be kept. Such as `humidity` metadata will be the `old values` received from EdgeX message bus.
- For the reading that can NOT be found in original message,  the metadata will not be set.  Such as metadata of `t1` in the sample will fill with default value that generated by eKuiper. 
- If your SQL has aggregated function, then it does not make sense to keep these metadata, but eKuiper will still fill with metadata from a particular message in the time window. For example, with following SQL, 
```SELECT avg(temperature) AS temperature, meta(*) AS edgex_meta FROM ... GROUP BY TUMBLINGWINDOW(ss, 10)```. 
In this case, there are possibly several messages in the window, the metadata value for `temperature` will be filled with value from 1st message that received from bus.

