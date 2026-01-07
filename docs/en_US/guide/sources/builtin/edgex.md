# EdgeX Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

eKuiper's EdgeX connector seamlessly integrates with EdgeX instances, allowing for efficient data ingestion and output. While it can function both as a source and a [sink connector](../../sinks/builtin/edgex.md), this section focuses on its role as a source connector.

eKuiper's EdgeX source connector can subscribe to the message from [EdgeX message bus](https://github.com/edgexfoundry/go-mod-messaging) and feed into the eKuiper streaming process pipeline. eKuiper's EdgeX source connector is tailored to consume events directly from EdgeX, ensuring effective stream processing without any manual schema definitions, thanks to EdgeX's predefined data types in its reading objects.

## Configurations

The connector in eKuiper can be configured with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API](../../../api/restapi/configKey.md), or configuration file. This section focuses on configuring eKuiper connectors with the configuration file.

eKuiper's default EdgeX source configuration resides at `$ekuiper/etc/sources/edgex.yaml`. This configuration serves as a base for all EdgeX connections. However, for specific use cases, you might need [custom configurations](#custom-configurations). eKuiper's [connector selector](../../connector.md#connection-selector) further enhances this by allowing connection reuse across configurations.

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

Users can specify the global EdgeX configurations here. The configuration items specified in `default` section will be taken as default configurations for all EdgeX connections.

### Connection Configurations

- `protocol`: The protocol connects to EdgeX message bus, default value is `tcp`.
- `server`: The server address of EdgeX message bus, default value is `localhost`.
- `port`: The port of EdgeX message bus, default value is `5573`.

### Connection Reusability

- `connectionSelector`: Specify the stream to reuse the connection to EdgeX message bus. For example, `edgex.redisMsgBus` in the below example. Note: The connection profile is located in `connections/connection.yaml`. More details can be found at [Connection Selector](../../connector.md#connection-selector).

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

  ::: tip

  If a connectionSelector is specified in a configuration group, all connection-related parameters will be ignored. This includes `protocol`, `server`, and `port`. In this case, the values `protocol: tcp | server: localhost | port: 5573` will not be used.

  :::

### Topic and Message

- `topic`: The topic name of EdgeX message bus, default value is `rules-events`. Users can subscribe to the topics of the message bus directly or subscribe to topics exported by EdgeX application service. Note that, the message types of the two types of topics are different, remember to set the appropriate messageType property.

- `type`: The EdgeX message bus type. Currently, three types of message buses are supported. `Redis` is used by default if no other value is specified.
  - `zero`: Use ZeroMQ as EdgeX message bus.
  - `mqtt`: Use the MQTT broker as EdgeX message bus. See [Optional Configuration (Specifically for MQTT)](#optional-configuration-specifically-for-mqtt) for more MQTT-related configurations.
  - `redis`: Use Redis as the EdgeX message bus. Redis is the default message bus when using EdgeX docker compose.

  EdgeX Levski introduces two types of information message bus, eKuiper supports these two new types from 1.7.1, respectively:
  - `nats-jetstream`
  - `nats-core`

- `messageType`: The EdgeX message model type.
  - `event`: If connected to the topic of EdgeX application service, the message model is an "event". The message will be decoded as a `dtos.Event` type. This is the default.
  - `request`: If connected to the topic of EdgeX message bus directly to receive the message from device service or core data, the message is a "request". The message will be decoded as a `requests.AddEventRequest` type.

### Optional Configuration (Specifically for MQTT)

If the MQTT message bus is used, additional optional configurations can be specified. Note that all optional values are strings, so configuration values should be enclosed in quotes. For example: `KeepAlive: "5000"`. The following optional MQTT configurations are supported. Refer to the MQTT specification for details on each option:

- `ClientId`

- `Username`
- `Password`
- `Qos`
- `KeepAlive`
- `Retained`
- `ConnectionPayload`
- `CertFile`
- `KeyFile`
- `CertPEMBlock`
- `KeyPEMBlock`
- `SkipCertVerify`

## Custom Configurations

For scenarios where you need to consume messages from multiple topics or customize certain connection parameters, eKuiper allows the creation of custom configuration profiles. By doing this, you can have multiple sets of configurations, each tailored for a specific use case.

Here's how to set up a custom configuration:

```yaml
#Override the global configurations
demo1: #Conf_key
  protocol: tcp
  server: 10.211.55.6
  port: 5571
  topic: rules-events
```

In the above example, a custom configuration named `demo1` is created. To utilize this configuration when creating a stream, use the `CONF_KEY` option and specify the configuration name. More details can be found at [Stream Statements](../../../sqls/streams.md).

**Usage Example**

```sql
create stream demo1() WITH (FORMAT="JSON", type="edgex", CONF_KEY="demo1");
```

Parameters defined in a custom configuration will override the corresponding parameters in the `default` configuration. Make sure to set values carefully to ensure the desired behavior.

## Create a Stream Source

Having set up the EdgeX source connector, the subsequent step involves its integration into eKuiper rules. This integration facilitates the processing of streamed data from EdgeX.

::: tip

edgeX Source connector can function as a [stream source](../../streams/overview.md) or a [scan table](../../tables/scan.md) source. This section illustrates the integration using the EdgeX Source connector as a stream source example.

:::

You can define the edgeX source as the data source either by REST API or CLI tool.

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, perfect for those looking to automate tasks or integrate eKuiper operations into other systems.

Example:

```sql
create stream demo1() WITH (FORMAT="JSON", type="edgex", CONF_KEY="demo1");
```

More details can be found at [Streams Management with REST API](../../../api/restapi/streams.md).

### Use CLI

For those who prefer a hands-on approach, the Command Line Interface (CLI) provides direct access to eKuiper's operations.

1. Navigate to the eKuiper binary directory:

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. Use the `create` command to create a rule, specifying the EdgeX connector as its source, for example:

   ```bash
   bin/kuiper CREATE STREAM demo'() with(format="json", datasource="demo" type="edgex")'
   ```

More details can be found at [Streams Management with CLI](../../../api/cli/streams.md).

### Further Reading: Stream Definition for EdgeX

When integrating eKuiper with EdgeX, it's recommended to use [schema-less stream](../../streams/overview.md#schema) definitions, as EdgeX has predefined data structures in its [reading objects](https://docs.edgexfoundry.org/2.0/microservices/core/data/Ch-CoreData/#events-and-readings).

For example, to define a stream in eKuiper that consumes events from EdgeX:

```shell
# cd $eKuiper_base
# bin/kuiper CREATE STREAM demo'() with(format="json", datasource="demo" type="edgex")'
```

### Automatic Data Type Conversion

When eKuiper processes events from EdgeX, it automatically manages data type conversions based on EdgeX's `ValueType` field.

**Data Conversion:**

- If eKuiper identifies a matching type in EdgeX's readings, it converts the data.
- If no match is found, the original value remains unchanged.
- If a conversion fails, the value is dropped, and a warning logs in the system.

#### Boolean

If `ValueType` value of the reading is `Bool`, then eKuiper tries to convert it to `boolean` type.

- Converted to `true`: "1", "t", "T", "true", "TRUE", "True"
- Converted to `false`: "0", "f", "F", "false", "FALSE", "False"

#### Bigint

If `ValueType` value of the reading is `INT8`, `INT16`, `INT32`, `INT64`, `UINT`, `UINT8`, `UINT16`, `UINT32`, `UINT64` then eKuiper tries to convert to `Bigint` type.

#### Float

If `ValueType` value of the reading is `FLOAT32`, `FLOAT64`, then eKuiper tries to convert to `Float` type.

#### String

If `ValueType` value of the reading is `String`, then eKuiper tries to convert it to `String` type.

#### Boolean array

`Bool` array type in EdgeX will be converted to `boolean` array.

#### Bigint array

All of `INT8`, `INT16`, `INT32`, `INT64`, `UINT`, `UINT8`, `UINT16`, `UINT32`, `UINT64` array types in EdgeX will be converted to `Bigint` array.

#### Float array

All of `FLOAT32`, `FLOAT64` array types in EdgeX will be converted to `Float` array.
