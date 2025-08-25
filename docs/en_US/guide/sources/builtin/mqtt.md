# MQTT Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

MQTT (Message Queuing Telemetry Transport) is a protocol optimized for low bandwidth scenarios. Using the MQTT source stream, eKuiper subscribes to messages from the MQTT broker and channels them into its processing pipeline. This integration allows for real-time data processing directly from specified MQTT topics.

 In eKuiper, the MQTT connector can function as both a source connector (ingesting data from MQTT brokers) and a [sink connector](../../sinks/builtin/mqtt.md) (publishing data to MQTT brokers). This section specifically focuses on its role as a source connector.

## Configurations

The connector in eKuiper can be configured with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API](../../../api/restapi/configKey.md), or configuration file. This section focuses on configuring eKuiper connectors with the configuration file.

eKuiper's default MQTT source configuration resides at `$ekuiper/etc/mqtt_source.yaml`. This configuration serves as a [base for all MQTT connections](#global-configuration). However, for specific use cases, you might need [custom configurations](#custom-configurations). eKuiper's [connector selector](../../connector.md#connection-selector) further enhances this by allowing connection reuse across configurations.

See below for a demo configuration with the global configuration and a customized `demo_conf` section.

```yaml
#Global MQTT configurations
default:
  qos: 1
  server: "tcp://127.0.0.1:1883"
  #username: user1
  #password: password
  #certificationPath: /var/kuiper/xyz-certificate.pem
  #privateKeyPath: /var/kuiper/xyz-private.pem.key
  #rootCaPath: /var/kuiper/xyz-rootca.pem
  #insecureSkipVerify: true
  #connectionSelector: mqtt.mqtt_conf1
  # Decompress the payload with the specified compression method. Support `gzip`, `zstd` method now.                                                                                                                                                                                                                                        
  # decompression: ""


#Override the global configurations
demo_conf: #Conf_key
  qos: 0
  server: "tcp://10.211.55.6:1883"
```

## Global Configurations

Users can specify the global MQTT configurations here. The configuration items specified in the `default` section will serve as the default configurations for all MQTT connections.

### Connection Settings

- `qos`: The default subscription QoS level.
- `server`: The server for MQTT message broker.
- `username`: The username for MQTT connection.
- `password`: The password for MQTT connection.
- `protocolVersion`: MQTT protocol version. 3.1 (also referred to as MQTT 3) or 3.1.1 (also referred to as MQTT 4). If not specified, the default value is 3.1.
- `clientid`: The client id for MQTT connection. If not specified, an uuid will be used.

### Security and Authentication Settings

- `certificationPath`:  Specifies the path to the certificate, for example: `d3807d9fa5-certificate.pem`. This can be an absolute or relative path. The base path for a relative address depends on where the `kuiperd` command is executed.
  - If executed as `bin/kuiperd` from `/var/kuiper`, the base is `/var/kuiper`.
  - If executed as `./kuiperd` from `/var/kuiper/bin`, the base is `/var/kuiper/bin`.
- `privateKeyPath`: The location of the private key path, for example `d3807d9fa5-private.pem.key`. It can be an absolute path or a relative path.  For more detailed information, see `certificationPath`.
- `rootCaPath`: The location of root ca path. It can be an absolute path, or a relative path.
- `certficationRaw`: base64 encoded original text of Cert, use `certificationPath` first if both defined.
- `privateKeyRaw`: base64 encoded original text of Key, use `privateKeyPath` first if both defined.
- `rootCARaw`: base64 encoded original text of CA, use `rootCaPath` first if both defined.
- `insecureSkipVerify`: Controls whether to skip certificate verification. If set to `true`, verification is skipped; otherwise, the certificate is verified.

### **Connection Reusability**

- `connectionSelector`: Specify the stream to reuse the connection to the MQTT broker, for example, `mqtt.localConnection` in the below example.  Note: The connection profile is located in `connections/connection.yaml`. For a detailed explanation of the connection selection, see [Connection Selector](../../connector.md#connection-selector).

  ```yaml
  #Global MQTT configurations
  default:
    qos: 1
    server: "tcp://127.0.0.1:1883"
    #username: user1
    #password: password
    #certificationPath: /var/kuiper/xyz-certificate.pem
    #privateKeyPath: /var/kuiper/xyz-private.pem.key
    connectionSelector: mqtt.localConnection
  ```

  ::: tip

  Once specify the connectionSelector in specific configuration group , all connection related parameters will be ignored , in this case ``servers: [tcp://127.0.0.1:1883]``

  :::

You can check the connectivity of the corresponding sink endpoint in advance through the API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check)

### **Payload Handling**

- `decompression`: Decompress the payload with the specified compression method. Support `gzip`, `zstd` method now.

- `bufferLength`: Specify the maximum number of messages to be buffered in the memory. This is used to avoid the extra large memory usage that would cause out of memory error. Note that the memory usage will be varied to the actual buffer. Increase the length here won't increase the initial memory allocation so it is safe to set a large buffer length. The default value is 102400, that is if each payload size is about 100 bytes, the maximum buffer size will be about 102400 * 100B ~= 10MB.

### **KubeEdge Integration**

- `kubeedgeVersion`: kubeedge version number. Different version numbers correspond to different file contents.

- `kubeedgeModelFile`: The name of the kubeedge template file. The file is located in the specified etc/sources folder. The sample format is as follows:

  ```yaml
  {
    "deviceModels": [{
      "name": "device1",
      "properties": [{
        "name": "temperature",
        "dataType": "int"
      }, {
        "name": "temperature-enable",
        "dataType": "string"
      }]
    }]
  }
  ```

  - `deviceModels.name`: The device name. It matches the field in the subscription topic that is located between the third and fourth "/". For example: $ke/events/device/device1/data/update.

  - `properties.name`: Field name.

  - `properties.dataType`: Expected field type.

## Custom Configurations

For scenarios where you need to customize certain connection parameters, eKuiper allows the creation of custom configuration profiles. By doing this, you can have multiple sets of configurations, each tailored for a specific use case.

Here's how to set up a custom configuration:

```json
#Override the global configurations
demo_conf: #Conf_key
  qos: 0
  server: "tcp://10.211.55.6:1883"
```

In the above example, a custom configuration named `demo_conf` is created. To utilize this configuration when creating a stream, use the `CONF_KEY` option and specify the configuration name. More details can be found at [Stream Statements](../../../sqls/streams.md).

**Usage Example**

```text
demo (
    ...
  ) WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo_conf");
```

Parameters defined in a custom configuration will override the corresponding parameters in the `default` configuration. Make sure to set values carefully to ensure the desired behavior.

## Create a Stream Source

Having defined the connector, the next phase involves its integration with eKuiper rules by creating a stream.

::: tip

MQTT Source connector can function as a [stream source](../../streams/overview.md) or a [scan table](../../tables/scan.md) source. This section illustrates the integration using the MQTT Source connector as a stream source example.

:::

You can define the MQTT source as the data source either by REST API or CLI tool.

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, perfect for those looking to automate tasks or integrate eKuiper operations into other systems.

Example:

```sql
{"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

In the example, the `WITH` clause provides specific configurations for the stream.

More details can be found at [Streams Management with REST API](../../../api/restapi/streams.md).

### Use CLI

For those who prefer a hands-on approach, the Command Line Interface (CLI) provides direct access to eKuiper's operations.

1. Navigate to the eKuiper binary directory:

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. Use the `create` command to create a rule, specifying the MQTT connector as its source, for example:

   ```bash
   bin/kuiper create stream my_stream '(id bigint, name string, score float) WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id")'
   ```

More details can be found at [Streams Management with CLI](../../../api/cli/streams.md).

## Migration Guide

Starting from version 1.5.0, eKuiper has modified the MQTT source broker configuration, transitioning from `servers` to `server`. As a result, users can now specify only a single MQTT broker address, as opposed to an array of addresses.

- If you've been using an MQTT broker as a stream source in earlier versions and plan to upgrade to 1.5.0 or subsequent releases, ensure that the `server` configuration in the `etc/mqtt_source.yaml` file is correctly set.
- If you've been relying on environment variables to determine the MQTT source address, an adjustment is required. For instance, if your broker address is `tcp://broker.emqx.io:1883`, then the environment variable should be changed from `MQTT_SOURCE__DEFAULT__SERVERS=[tcp://broker.emqx.io:1883]` to `MQTT_SOURCE__DEFAULT__SERVER="tcp://broker.emqx.io:1883"`


## Listening to Multiple Topics

When creating a stream, we can use the following method to listen to multiple topics simultaneously:

```sql
{"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"t1,t2\", FORMAT = \"json\", KEY = \"id\")"}
```
