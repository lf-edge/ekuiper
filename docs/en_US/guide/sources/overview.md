# Source Connectors

In the eKuiper source code, there are built-in sources and sources in extension.

## Ingestion Mode

The source connector provides the connection to an external system to load data in. Regarding data loading mechanism, there are two modes:

- Scan: load the data events one by one like a stream which is driven by event. Such mode of source can be used in stream or scan table.
- Lookup: refer to external content when needed, only used in lookup table.

Each source will support one or both modes. In the source page, a badge will show if the mode is supported.

## Built-in Sources

Users can directly use the built-in sources in the standard eKuiper instance. The list of built-in sources is as follows:

- [MQTT source](./builtin/mqtt.md): read data from MQTT topics.
- [Neuron source](./builtin/neuron.md): read data from the local neuron instance.
- [EdgeX source](./builtin/edgex.md): read data from EdgeX foundry.
- [HTTP pull source](./builtin/http_pull.md): source to pull data from HTTP servers.
- [Http push source](./builtin/http_push.md): push data to eKuiper through http.
- [Redis source](./builtin/redis.md): source to lookup from Redis as a lookup table.
- [RedisSub source](./builtin/redisSub.md): subscribe data from Redis channels.
- [File source](./builtin/file.md): source to read from file, usually used as tables.
- [Memory source](./builtin/memory.md): source to read from eKuiper memory topic to form rule pipelines.
- [Simulator source](./builtin/simulator.md): source to generate mock data for testing.

## Predefined Source Plugins

We have developed some official source plugins. These plugins can be found in eKuiper's source code and users need to build them manually. Please check each source about how to build and use.

Additionally, these plugins have pre-built binaries for the mainstream CPU architecture such as AMD or ARM. The pre-built plugin is hosted in `https://packages.emqx.net/kuiper-plugins/$version/$os/sources/$type_$arch.zip`. For example, to get the ZMQ source for Debian amd64, install it from `https://packages.emqx.net/kuiper-plugins/1.4.4/debian/sources/zmq_amd64.zip`.

The list of predefined source plugins:

- [SQL source](./plugin/sql.md): a source to periodically fetch data from SQL DB.
- [Video Source](./plugin/video.md): a source to query video streams.
- [Random source](./plugin/random.md): a source to generate random data for testing.
- [Zero MQ source](./plugin/zmq.md): read data from zero mq.
- [Kafka source](./plugin/kafka.md): read data from Kafka.

## Use of Sources

The user uses sources by means of streams or tables. The type `TYPE` property needs to be set to the name of the desired source in the stream properties created. The user can also change the behavior of the source during stream creation by configuring various general source attributes, such as the decoding type (default is JSON), etc. For the general properties and creation syntax supported by creating streams, please refer to the [Stream Specification](../streams/overview.md).

## Runtime Nodes

When users create rules, the data source is a logical node. Depending on the type of the data source itself and the
user's configuration, each data source at runtime may generate an execution plan consisting of multiple nodes. The data
source property configuration items are numerous, and the logic during actual runtime is quite complex. By breaking down
the execution plan into multiple nodes, the following benefits are primarily achieved:

- There are many shared properties and implementation logic among various data sources, such as data format decoding.
  Splitting the shared property implementation into independent runtime nodes facilitates node reuse, simplifies the
  implementation of data source nodes (Single Responsibility Principle), and improves the maintainability of nodes.
- The properties of the data source include time-consuming calculations, such as decompression and decoding. With a
  single node's metrics, it is difficult to distinguish the actual execution status of sub-tasks when the data source is
  executed. After splitting the nodes, finer-grained runtime metrics can be supported to understand the status and
  latency of each sub-task.
- After sub-task splitting, parallel computation can be implemented, improving the overall efficiency of rule execution.

### Execution Plan

The physical execution plan of the data source node can be split into:

Connector --> RateLimit --> Decompress --> Decode --> Preprocess

The conditions for generating each node are:

- **Connector**: Implemented for every data source, used to connect to external data sources and read data into the
  system.
- **RateLimit**: Applicable when the data source type is a push source (such as MQTT, a source that reads data in
  through subscription/push rather than pull) and the `interval` property is configured. This node is used to control
  the frequency of data inflow at the data source. For details, please refer to [Down Sampling](./down_sample.md).
- **Decompress**: Applicable when the data source type reads bytecode data (such as MQTT, which allows sending any
  bytecode rather than a fixed format) and the `decompress` property is configured. This node is used to decompress the
  data.
- **Decode**: Applicable when the data source type reads bytecode data and the `format` property is configured. This
  node will deserialize the bytecode based on the format configuration and schema-related configuration.
- **Preprocess**: Applicable when a schema is explicitly defined in the stream definition and `strictValidation` is
  turned on. This node will validate and transform the raw data according to the schema definition. Note that if type
  conversion is frequently required for the input data, this node may incur significant additional performance overhead.
