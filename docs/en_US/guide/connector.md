# Connectors

In the realm of stream processing, the ability to seamlessly interact with various data sources and sinks is of paramount importance. eKuiper, being a lightweight edge stream processing engine, recognizes this necessity and incorporates it through the use of "connectors."

Connectors in eKuiper serve as the bridge between the processing engine and external systems, including databases, message brokers, or other data stores. By leveraging connectors, eKuiper can ingest data from diverse sources, process it in real-time, and then dispatch the processed data to the desired destinations. This ensures that eKuiper can be integrated into a wide variety of environments and use cases, from IoT edge devices to cloud-based infrastructures. They are categorized into two primary types:

- **Source Connectors**: Responsible for ingesting data into the eKuiper platform from various external sources.
- **Sink Connectors**: Handle the dispatch of processed data from eKuiper to external sinks or endpoints.

The architecture of eKuiper provides both built-in connectors, which cater to common data sources and sinks, and plugin-based connectors, allowing for extensibility and integration with custom or third-party systems. Proper configuration and management of these connectors are crucial for the efficient and reliable operation of the eKuiper platform.

This chapter details the configuration, usage, and best practices associated with both source and sink connectors in eKuiper.

## Source Connectors

[Source connectors](./sources/overview.md) in eKuiper are designed to facilitate data ingestion from various external sources into the platform. Within the eKuiper framework, each source can operate in one of two distinct modes: a "Streaming" mode, where data events are streamed sequentially, or a "Reference" mode (often used in the context of 'tables'), where specific external content is fetched based on queries. To leverage these capabilities, users simply integrate these sources into their streams or tables, specifying the desired source type and fine-tuning behavior through configurable attributes.

**Built-in Source Connectors**

Below are the built-in source connectors provided by eKuiper:

- [MQTT source](./sources/builtin/mqtt.md): A source to read data from MQTT topics.
- [Neuron source](./sources/builtin/neuron.md): A source to read data from the local neuron instance.
- [EdgeX source](./sources/builtin/edgex.md): A source to read data from EdgeX foundry.
- [HTTP pull source](./sources/builtin/http_pull.md): A source to pull data from HTTP servers.
- [HTTP push source](./sources/builtin/http_push.md): A source to push data to eKuiper through HTTP.
- [File source](./sources/builtin/file.md): A source to read from file, usually used as tables.
- [Memory source](./sources/builtin/memory.md): A source to read from eKuiper memory topic to form rule pipelines.
- [Redis source](./sources/builtin/redis.md): A source to lookup from Redis as a lookup table.

**Plugin-based Source Connectors**

For scenarios where custom data sources or specific third-party integrations are needed, eKuiper offers the flexibility of plugin-based source connectors:

- [SQL source](./sources/plugin/sql.md): A source to periodically fetch data from SQL DB.
- [Video Source](./sources/plugin/video.md): A source to query video streams.
- [Random source](./sources/plugin/random.md): A source to generate random data for testing.
- [Zero MQ source](./sources/plugin/zmq.md): A source to read data from Zero MQ.
- [Kafka source](./sources/plugin/kafka.md): A source to read data from Kafka

## Sink Connectors

Sink connectors handle the task of dispatching the processed data from eKuiper to various external endpoints or sinks. These sinks can directly interface with platforms like MQTT, Neuron, and EdgeX, among others, while also offering cache mechanisms to handle network interruptions and ensure data consistency. Additionally, users have the flexibility to customize sink behaviors through dynamic properties and resource reuse, streamlining integration and improving scalability.

Similar to source connectors, sink connectors are also categorized into built-in and plugin-based types.

**Built-in Sink Connectors**

Below are the built-in sink connectors provided by eKuiper:

- [MQTT sink](./sinks/builtin/mqtt.md): A sink to external MQTT broker.
- [Neuron sink](./sinks/builtin/neuron.md): A sink to the local neuron instance.
- [EdgeX sink](./sinks/builtin/edgex.md): A sink to EdgeX Foundry. This sink only exists when enabling the edgex build tag.
- [Rest sink](./sinks/builtin/rest.md): A sink to external HTTP server.
- [Redis sink](./sinks/builtin/redis.md): A sink to Redis.
- [File sink](./sinks/builtin/file.md): A sink to a file.
- [Memory sink](./sinks/builtin/memory.md): A sink to eKuiper memory topic to form rule pipelines.
- [Log sink](./sinks/builtin/log.md): A sink to log, usually for debugging only.
- [Nop sink](./sinks/builtin/nop.md): A sink to nowhere. It is used for performance testing now.

### Plugin-based Sink Connectors

For specialized data dispatch requirements or integrations with particular platforms, eKuiper supports plugin-based sink connectors:

- [InfluxDB sink](./sinks/plugin/influx.md): A sink to InfluxDB `v1.x`.
- [InfluxDBV2 sink](./sinks/plugin/influx2.md): A sink to InfluxDB `v2.x`.
- [Image sink](./sinks/plugin/image.md): A sink to an image file. Only used to handle binary results.
- [Zero MQ sink](./sinks/plugin/zmq.md): A sink to Zero MQ.
- [Kafka sink](./sinks/plugin/kafka.md): A sink to Kafka.

### Data Templates in Sink Connectors

[Data templates](./sinks/data_template.md) in eKuiper allow for "secondary processing" of analysis results to cater to the diverse formatting requirements of different sink systems. Utilizing the Golang template system, eKuiper provides mechanisms for dynamic data transformation, conditional outputs, and iterative processing. This ensures compatibility and precise formatting for various sinks.

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
