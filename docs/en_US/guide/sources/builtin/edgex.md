## EdgeX Source Connector

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper's EdgeX connector seamlessly integrates with EdgeX instances, allowing for efficient data ingestion and output. While it can function both as a source and a [sink connector](../../sinks/builtin/edgex.md), this section delves into its capabilities as a source connector.

eKuiper's EdgeX source connector can subscribe to the message from [EdgeX message bus](https://github.com/edgexfoundry/go-mod-messaging) and feed into the eKuiper streaming process pipeline. eKuiper's EdgeX source connector is tailored to consume events directly from EdgeX, ensuring effective stream processing without any manual schema definitions, thanks to EdgeX's predefined data types in its reading objects.

## Configure EdgeX Connector

The connector in eKuiper can be configured with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API](../../../api/restapi/configKey.md) or configuration file. This section focuses on configuring eKuiper connectors with the configuration file. 

eKuiper's default MQTT source configuration resides at `$ekuiper/etc/sources/edgex.yaml`. This configuration serves as a base for all MQTT connections. However, for specific use cases, you might need [custom configurations](#custom-configurations). eKuiper's [connector selector](../../connector.md#connection-selector) further enhances this by allowing connection reuse across configurations.

See below for a demo configuration with the global configuration and a customized `demo1` section. 

```yaml
#Global Edgex configurations
default:
  protocol: tcp
  server: localhost
  port: 5573
  topic: rules-events
  messageType: event
#  optional:
#    ClientId: client1
#    Username: user1
#    Password: password

#Override the global configurations
demo1: #Conf_key
  protocol: tcp
  server: 10.211.55.6
  port: 5571
  topic: rules-events
```

## Global Configuration

Use can specify the global MQTT configurations here. The configuration items specified in `default` section will be taken as default configurations for all EdgeX connections.

protocol:  The protocol connect to EdgeX message bus, default value is `tcp`.

server: The server address of  EdgeX message bus, default value is `localhost`.

port: The port of EdgeX message bus, default value is `5573`.

connectionSelector: specify the stream to reuse the connection to EdgeX message bus. The connection profile located in `connections/connection.yaml`.

```yaml
edgex:
  redisMsgBus: #connection key
    protocol: redis
    server: 127.0.0.1
    port: 6379
    type: redis
    #  Below is optional configurations settings for mqtt
    #  type: mqtt
    #  optional:
    #    ClientId: client1
    #    Username: user1
    #    Password: password
    #    Qos: 1
    #    KeepAlive: 5000
    #    Retained: true/false
    #    ConnectionPayload:
    #    CertFile:
    #    KeyFile:
    #    CertPEMBlock:
    #    KeyPEMBlock:
    #    SkipCertVerify: true/false
```

There is one configuration group for EdgeX message bus in the example, user need use `edgex.redisMsgBus` as the selector.
For example

```yaml
#Global Edgex configurations
default:
  protocol: tcp
  server: localhost
  port: 5573
  connectionSelector: edgex.redisMsgBus
  topic: rules-events
  messageType: event
  #  optional:
  #    ClientId: client1
  #    Username: user1
  #    Password: password
  
  
```

*Note*: once specify the connectionSelector in specific configuration group , all connection related parameters will be ignored , in this case `protocol: tcp | server: localhost | port: 5573`

topic:  The topic name of EdgeX message bus, default value is `rules-events`. Users can subscribe to the topics of message bus
directly or subscribe to topics exported by EdgeX application service. Notice that, the message type of the two types of
topics are different, remember to set the appropriate messageType property.

type: The EdgeX message bus type, currently three types of message buses are supported. If specified other values, then will
use the default `redis` value.

- `zero`: Use ZeroMQ as EdgeX message bus.
- `mqtt`: Use the MQTT broker as EdgeX message bus.
- `redis`: Use Redis as EdgeX message bus. When using EdgeX docker compose, the type will be set to this by default.

EdgeX Levski introduces two types of information message bus, eKuiper supports these two new types from 1.7.1, respectively

- `nats-jetstream`
- `nats-core`

messageType: The EdgeX message model type. If connected to the topic of EdgeX application service, the message model is an "event".
Otherwise, if connected to the topic of EdgeX message bus directly to receive the message from device service or core
data, the message is a "request". There are two available types of messageType property:

- `event`: The message will be decoded as a `dtos.Event` type. This is the default.
- `request`: The message will be decoded as a `requests.AddEventRequest` type.

optional: If MQTT message bus is used, some other optional configurations can be specified. Please notice that all of values in
optional are **<u>string type</u>**, so values for these configurations should be string - such as `KeepAlive: "5000"`
. Below optional configurations are supported, please check MQTT specification for the detailed information.

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

### Custom Configurations

In some cases, maybe you want to consume message from multiple topics from message bus.  eKuiper supports to specify another configuration, and use the `CONF_KEY` to specify the newly created key when you create a stream.

```yaml
#Override the global configurations
demo1: #Conf_key
  protocol: tcp
  server: 10.211.55.6
  port: 5571
  topic: rules-events
```

If you have a specific connection that need to overwrite the default settings, you can create a customized section. In the previous sample, we create a specific setting named with `demo1`.  Then you can specify the configuration with option `CONF_KEY` when creating the stream definition (see [stream specs](../../../sqls/streams.md) for more info).

**Sample**

```sql
create stream demo1() WITH (FORMAT="JSON", type="edgex", CONF_KEY="demo1");
```

The configuration keys used for these specific settings are the same as in `default` settings, any values specified in specific settings will overwrite the values in `default` section.

### Stream definition for EdgeX

When integrating eKuiper with EdgeX, it's recommended to use schema-less stream definitions, as EdgeX has predefined data structures in its [reading objects](https://docs.edgexfoundry.org/2.0/microservices/core/data/Ch-CoreData/#events-and-readings).

For example, to define a stream in eKuiper that consumes events from EdgeX:

```shell
# cd $eKuiper_base
# bin/kuiper CREATE STREAM demo'() with(format="json", datasource="demo" type="edgex")'
```

### Automatic Data Type Conversion

When eKuiper processes events from EdgeX, it automatically manages data type conversions based on EdgeX's 'ValueType' field.

Data Conversion:

- If eKuiper identifies a matching type in EdgeX's readings, it converts the data.
- If no match is found, the original value remains unchanged.
- If a conversion fails, the value is dropped, and a warning logs in the system.

The types defined in readings will be converted into related [data types](../../../sqls/streams.md) that are supported in eKuiper.

The table below provides a concise mapping:

|     **EdgeX ValueType**      | **eKuiper Data Type** |
| :--------------------------: | :-------------------: |
|             Bool             |        boolean        |
|    INT8, INT16... UINT64     |        Bigint         |
|       FLOAT32, FLOAT64       |         Float         |
|            String            |        String         |
|          Bool array          |     boolean array     |
|  INT8 array... UINT64 array  |     Bigint array      |
| FLOAT32 array, FLOAT64 array |      Float array      |

::: tip 

#### Boolean

If `ValueType` value of the reading is `Bool`, then eKuiper tries to convert to `boolean` type. Following values will be converted into `true`.

- "1", "t", "T", "true", "TRUE", "True"

Following will be converted into `false`.

- "0", "f", "F", "false", "FALSE", "False"

#### Bigint

If `ValueType` value of the reading is `INT8`, `INT16`, `INT32`, `INT64`, `UINT`, `UINT8`, `UINT16`, `UINT32`, `UINT64` then eKuiper tries to convert to `Bigint` type.

#### Float

If `ValueType` value of the reading is `FLOAT32`, `FLOAT64`, then eKuiper tries to convert to `Float` type.

#### String

If `ValueType` value of the reading is `String`, then eKuiper tries to convert to `String` type.

#### Boolean array

`Bool` array type in EdgeX will be converted to `boolean` array.

#### Bigint array

All of `INT8`, `INT16`, `INT32`, `INT64`, `UINT`, `UINT8`, `UINT16`, `UINT32`, `UINT64` array types in EdgeX will be converted to `Bigint` array.

#### Float array

All of `FLOAT32`, `FLOAT64` array types in EdgeX will be converted to `Float` array.





## Integrate EdgeX Source with eKuiper Rules

Having set up the EdgeX source connector, the subsequent step involves its integration into eKuiper rules. This integration facilitates the processing of streamed data from EdgeX.

### Using REST API

(Provide steps and example on setting up EdgeX source using eKuiper's REST API.)

### Using CLI

(Provide steps and example on setting up EdgeX source using eKuiper's Command Line Interface.)

