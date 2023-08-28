# MQTT Source Connector

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

MQTT (Message Queuing Telemetry Transport) is a protocol optimized for low bandwidth scenarios. In eKuiper, the MQTT connector can function both as a source connector (ingesting data from MQTT brokers) and a [sink connector](../../sinks/builtin/mqtt.md) (publishing data to MQTT brokers). This section specifically focuses on its role as a source connector. 

Using the MQTT source stream, eKuiper subscribes to messages from the MQTT broker and channels them into its processing pipeline. This integration allows for real-time data processing directly from specified MQTT topics.

## Configure MQTT Connector

The connector in eKuiper can be configured with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API](../../../api/restapi/configKey.md) or configuration file. This section focus on configuring eKuiper connectors with the configuration file. 

eKuiper's default MQTT source configuration resides at `$ekuiper/etc/mqtt_source.yaml`. This configuration serves as a [base for all MQTT connections](#global-mqtt-connector). However, for specific use cases, you might need [custom configurations](#custom-configurations). eKuiper's [connector selector](#connection-selector) further enhances this by allowing connection reuse across configurations.

See below for a demo configuration with the global configuration and a customized demo_conf section. 

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

## Global MQTT Connector

Use can specify the global MQTT configurations here. The configuration items specified in `default` section will be taken as default configurations for all MQTT connections.

### Connection Settings

- `qos`: The default subscription QoS level.
- `server`: The server for MQTT message broker.
- `username`: The username for MQTT connection.
- `password`: The password for MQTT connection.
- `protocolVersion`: MQTT protocol version. 3.1 (also refer as MQTT 3) or 3.1.1 (also refer as MQTT 4). If not specified, the default value is 3.1.
- `clientid`: The client id for MQTT connection. If not specified, an uuid will be used.

### Security and Authentication Settings

- `certificationPath`: The location of certification path. It can be an absolute path, or a relative path. If it is an relative path, then the base path is where you excuting the `kuiperd` command. For example, if you run `bin/kuiperd` from `/var/kuiper`, then the base path is `/var/kuiper`; If you run `./kuiperd` from `/var/kuiper/bin`, then the base path is `/var/kuiper/bin`.  Such as  `d3807d9fa5-certificate.pem`.
- `privateKeyPath`: The location of private key path. It can be an absolute path, or a relative path.  For more detailed information, please refer to `certificationPath`. Such as `d3807d9fa5-private.pem.key`.
- `rootCaPath`: The location of root ca path. It can be an absolute path, or a relative path.
- `insecureSkipVerify`: Control if to skip the certification verification. If it is set to true, then skip certification verification; Otherwise, verify the certification

### **Connection Reusability**

- `connectionSelector`: Specify the stream to reuse the connection to the MQTT broker. For a detailed explanation of the connection selection, see [Connection Selector](#connection-selector). For example,`mqtt.localConnection` in the below example. 

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

### **Payload Handling**

- `decompression`: Decompress the payload with the specified compression method. Support `gzip`, `zstd` method now.

- `bufferLength`: Specify the maximum number of messages to be buffered in the memory. This is used to avoid the extra large memory usage that would cause out of memory error. Note that the memory usage will be varied to the actual buffer. Increase the length here won't increase the initial memory allocation so it is safe to set a large buffer length. The default value is 102400, that is if each payload size is about 100 bytes, the maximum buffer size will be about 102400 * 100B ~= 10MB.

### **KubeEdge Integration**

- `kubeedgeVersion`: kubeedge version number. Different version numbers correspond to different file contents.

- `kubeedgeModelFile`: The name of the kubeedge template file. The file is located in the specified etc/sources folder. The sample format is as follows:

  ```json
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

For scenarios requiring deviations from the defaults, eKuiper lets you override global settings. For example, with the `demo_conf` configuration. Then you can specify the configuration with option `CONF_KEY` when creating the stream definition (see [stream specs](../../../sqls/streams.md) for more info). <!--这里是在讲通过rest api 来配置吗？链接到 stream 是？-->

**Sample**

```text
demo (
    ...
  ) WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo_conf");
```

The configuration keys used for these specific configurations are the same as in `default` configurations, any values specified in specific configurations will overwrite the values in `default` section.

## Connection Selector

The connector selector is a powerful feature in eKuiper that allows users to define a connection once and reuse it across multiple configurations. It ensures efficient connection management and reduces redundancy.

To define a global connection configuration, use the `connectionSelector` key to name your connection, e.g., `mqtt.localConnection`. Override global configurations with custom configurations but reference the same `connectionSelector`.

For example, consider the configurations `demo_conf` and `demo2_conf`:

```yaml
#Override the global configurations
demo_conf: #Conf_key
  qos: 0
  connectionSelector: mqtt.localConnection 
  servers: [tcp://10.211.55.6:1883, tcp://127.0.0.1]

#Override the global configurations
demo2_conf: #Conf_key
  qos: 0
  connentionSelector: mqtt.localConnection
  servers: [tcp://10.211.55.6:1883, tcp://127.0.0.1]
```

Both configurations reference the same `connectionSelector`, indicating that they utilize the same MQTT connection. When streams `demo` and `demo2` are defined based on these configurations:

```text
demo (
    ...
  ) WITH (DATASOURCE="test/", FORMAT="JSON", CONF_KEY="demo_conf");

demo2 (
    ...
  ) WITH (DATASOURCE="test2/", FORMAT="JSON", CONF_KEY="demo2_conf");

```

They inherently share the MQTT connection. Specifically:

- The stream `demo` subscribes to the MQTT topic `test/` with a QoS of 0.
- The stream `demo2` subscribes to `test2/`, also with a QoS of 0.

::: tip

However, if two streams have the same `DATASOURCE` but differing `qos` values, only the rule started first will trigger a subscription.

:::

The actual connection profiles, like `mqtt.localConnection`, are usually defined in a separate file, such as `connections/connection.yaml`. 

Example

```yaml
mqtt:
  localConnection: #connection key
    server: "tcp://127.0.0.1:1883"
    username: ekuiper
    password: password
    #certificationPath: /var/kuiper/xyz-certificate.pem
    #privateKeyPath: /var/kuiper/xyz-private.pem.ke
    #insecureSkipVerify: false
    #protocolVersion: 3
    clientid: ekuiper
  cloudConnection: #connection key
    server: "tcp://broker.emqx.io:1883"
    username: user1
    password: password
    #certificationPath: /var/kuiper/xyz-certificate.pem
    #privateKeyPath: /var/kuiper/xyz-private.pem.ke
    #insecureSkipVerify: false
    #protocolVersion: 3
```



## Integrate MQTT Source with eKuiper Rules

With the connector defined, the next step is integrating it into eKuiper rules to start processing the streamed data.

**Steps to Integrate**:

1. Define a rule that specifies the MQTT connector as its source.
2. In the rule, mention the desired MQTT topic and the processing logic.

::: tip

MQTT Source connector can function as a [stream source](../../streams/overview.md) or a [scan table](../../tables/scan.md) source. This section illustrates the integration using the MQTT Source connector as a stream source example.

:::

You can define the MQTT source as the data source either by REST API or CLI tool. 

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, perfect for those looking to automate tasks or integrate eKuiper operations into other systems.

- Steps to Use

  1. Use the appropriate REST endpoint to define the MQTT connector.
  
  2. Use another endpoint to create a rule that utilizes the MQTT connector, for example
  
     ```json
     {"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
     ```
  
     In the example, the `WITH` clause provides specific configurations for the stream. for detailed explanation of each field, see [Streams management with REST API](../../../api/restapi/streams.md)

### Use CLI

For those who prefer a hands-on approach, the Command Line Interface (CLI) provides direct access to eKuiper's operations.

- Steps to Use

  1. Use the `create` command to define the MQTT connector.

  2. Use the `rule` command to create a rule, specifying the MQTT connector as its source, for example
  
     ```bash
     bin/kuiper create stream my_stream '(id bigint, name string, score float) WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id")'
     ```

For detailed operating steps, see [Streams management with CLI](../../../api/cli/streams.md).

## Migration Guide

Starting from version 1.5.0, eKuiper has modified the MQTT source broker configuration, transitioning from `servers` to `server`. As a result, users can now specify only a single MQTT broker address, as opposed to an array of addresses.

- If you've been using an MQTT broker as a stream source in earlier versions and plan to upgrade to 1.5.0 or subsequent releases, ensure that the `server` configuration in the `etc/mqtt_source.yaml` file is correctly set. 
- If you've been relying on environment variables to determine the MQTT source address, an adjustment is required. For instance, if your broker address is `tcp://broker.emqx.io:1883`, then the environment variable should be changed from `MQTT_SOURCE__DEFAULT__SERVERS=[tcp://broker.emqx.io:1883]` to `MQTT_SOURCE__DEFAULT__SERVER="tcp://broker.emqx.io:1883"`."
