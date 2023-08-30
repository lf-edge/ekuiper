# Connectors

[LF Edge eKuiper](https://www.lfedge.org/projects/ekuiper/) is an edge lightweight IoT data analytics/streaming software that can be run on all kinds of resource-constrained IoT devices. 

Connectors in eKuiper serve as the interface for data ingestion and dispatch within the stream processing framework. They are categorized into two primary types:

- **Source Connectors**: Responsible for ingesting data into the eKuiper platform from various external sources.
- **Sink Connectors**: Handle the dispatch of processed data from eKuiper to external sinks or endpoints.

The architecture of eKuiper provides both built-in connectors, which cater to common data sources and sinks, and plugin-based connectors, allowing for extensibility and integration with custom or third-party systems. Proper configuration and management of these connectors are crucial for the efficient and reliable operation of the eKuiper platform.

This chapter details the configuration, usage, and best practices associated with both source and sink connectors in eKuiper.

## Source Connectors

[Source connectors](./sources/overview.md) in eKuiper are designed to facilitate data ingestion from various external sources into the platform. Depending on the requirements, users can opt for built-in connectors or extend the system with plugin-based connectors. 

Each source operates in either a "Scan" mode, streaming data events sequentially, or a "Lookup" mode, referencing external content as needed. To utilize these sources, users define them in streams or tables, specifying the desired source type and configuring attributes to tailor source behavior.

### Built-in Source Connectors

Built-in source connectors are integrated directly within the eKuiper platform. These connectors:

- Provide immediate connectivity to commonly used data sources.
- Require minimal configuration and setup.
- Are maintained and updated with the core eKuiper system.

Below are the built-in source connectors provided by eKuiper:

- [MQTT source](./sources/builtin/mqtt.md): read data from MQTT topics.
- [Neuron source](./sources/builtin/neuron.md): read data from the local neuron instance.
- [EdgeX source](./sources/builtin/edgex.md): read data from EdgeX foundry.
- [HTTP pull source](./sources/builtin/http_pull.md): source to pull data from HTTP servers.
- [Http push source](./sources/builtin/http_push.md): push data to eKuiper through http.
- [Redis source](./sources/builtin/redis.md): source to lookup from Redis as a lookup table.
- [File source](./sources/builtin/file.md): source to read from file, usually used as tables.
- [Memory source](./sources/builtin/memory.md): source to read from eKuiper memory topic to form rule pipelines.

### Plugin-based Source Connectors

For scenarios where custom data sources or specific third-party integrations are needed, eKuiper offers the flexibility of plugin-based source connectors:

- [SQL source](./sources/plugin/sql.md): a source to periodically fetch data from SQL DB.
- [Video Source](./sources/plugin/video.md): a source to query video streams. 
- [Random source](./sources/plugin/random.md): a source to generate random data for testing.
- [Zero MQ source](./sources/plugin/zmq.md): read data from zero MQ.

**Plugin-based Source Connectors**

For scenarios where custom data sources or specific third-party integrations are needed, eKuiper offers the flexibility of plugin-based source connectors:

- Understand the architecture and lifecycle of a plugin-based connector.
- Learn the process to integrate third-party or custom source plugins.
- Explore configuration nuances specific to plugin-based sources.

## Sink Connectors

Sink connectors handle the task of dispatching the processed data from eKuiper to various external endpoints or sinks. Similar to source connectors, sink connectors are also categorized into built-in and plugin-based types.

These sinks can directly interface with platforms like MQTT, Neuron, and EdgeX, among others, while also offering cache mechanisms to handle network interruptions and ensure data consistency. 

Additionally, users have the flexibility to customize sink behaviors through dynamic properties and resource reuse, streamlining integration and improving scalability.

### Built-in Sink Connectors

Directly integrated into eKuiper, built-in sink connectors:

- Enable dispatch to widely used data sinks or platforms.
- Offer streamlined configuration options.
- Benefit from direct updates alongside eKuiper's core updates.

Below are the built-in source connectors provided by eKuiper:

- [MQTT sink](./sinks/builtin/mqtt.md): sink to external MQTT broker.
- [Neuron sink](./sinks/builtin/neuron.md): sink to the local neuron instance.
- [EdgeX sink](./sinks/builtin/edgex.md): sink to EdgeX Foundry. This sink only exists when enabling the edgex build tag.
- [Rest sink](./sinks/builtin/rest.md): sink to external HTTP server.
- [Redis sink](./sinks/builtin/redis.md): sink to Redis.
- [File sink](./sinks/builtin/file.md): sink to a file.
- [Memory sink](./sinks/builtin/memory.md): sink to eKuiper memory topic to form rule pipelines.
- [Log sink](./sinks/builtin/log.md): sink to log, usually for debugging only.
- [Nop sink](./sinks/builtin/nop.md): sink to nowhere. It is used for performance testing now.

### Plugin-based Sink Connectors

For specialized data dispatch requirements or integrations with particular platforms, eKuiper supports plugin-based sink connectors:

- [InfluxDB sink](./sinks/plugin/influx.md): sink to InfluxDB `v1.x`.
- [InfluxDBV2 sink](./sinks/plugin/influx2.md): sink to InfluxDB `v2.x`.
- [TDengine sink](./sinks/plugin/tdengine.md): sink to TDengine.
- [Image sink](./sinks/plugin/image.md): sink to an image file. Only used to handle binary results.
- [Zero MQ sink](./sinks/plugin/zmq.md): sink to Zero MQ.
- [Kafka sink](./sinks/plugin/kafka.md): sink to Kafka.

### Data Templates in Sink Connectors

[Data templates](./sinks/data_template.md) in eKuiper allow for "secondary processing" of analysis results to cater to the diverse formatting requirements of different sink systems. Utilizing the Golang template system, eKuiper provides mechanisms for dynamic data transformation, conditional outputs, and iterative processing. This ensures compatibility and precise formatting for various sinks. 

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
## Batch Configuration

For advanced data stream processing, eKuiper offers an array of connectors like Memory, File, MQTT, and more. To streamline the integration, eKuiper’s REST API introduces the capability for batch configuration, allowing users to simultaneously import or export multiple configurations.

Example 

```json
{
    "streams": { ... },
    "tables": { ... },
    "rules": { ... },
    "nativePlugins": { ... },
    "portablePlugins": { ... },
    "sourceConfig": { ... },
    "sinkConfig": { ... },
    ...
}
```

More details can be found at [Data Import/Export Management](../api/restapi/data.md)
