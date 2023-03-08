# Source Connectors

In the eKuiper source code, there are built-in sources and sources in extension.

## Ingestion Mode

The source connector provides the connection to an external system to load data in. Regarding data loading mechanism, there are two modes:

- Scan: load the data events one by one like a stream which is driven by event. Such mode of source can be used in stream or scan table.
- Lookup: refer to external content when needed, only used in lookup table.

Each source will support one or both modes. In the source page, a badge will show if the mode is supported.

## Built-in Sources

Users can directly use the built-in sources in the standard eKuiper instance. The list of built-in sources are:

- [Mqtt source](./builtin/mqtt.md): read data from mqtt topics.
- [Neuron source](./builtin/neuron.md): read data from the local neuron instance.
- [EdgeX source](./builtin/edgex.md): read data from EdgeX foundry.
- [Http pull source](./builtin/http_pull.md): source to pull data from http servers.
- [Http push source](./builtin/http_push.md): push data to eKuiper through http.
- [Redis source](./builtin/redis.md): source to lookup from redis as a lookup table.
- [File source](./builtin/file.md): source to read from file, usually used as tables.
- [Memory source](./builtin/memory.md): source to read from eKuiper memory topic to form rule pipelines.


## Predefined Source Plugins

We have developed some official source plugins. These plugins can be found in eKuiper's source code and users need to build them manually. Please check each source about how to build and use.

Additionally, these plugins have pre-built binaries for the mainstream cpu architecture such as AMD or ARM. The pre-built plugin hosted in `https://packages.emqx.net/kuiper-plugins/$version/$os/sources/$type_$arch.zip`. For example, to get zmq source for debian amd64, install it from `https://packages.emqx.net/kuiper-plugins/1.4.4/debian/sources/zmq_amd64.zip`.

The list of predefined source plugins:

- [SQL source](./plugin/sql.md): a source to periodically fetch data from SQL DB.
- [Random source](./plugin/random.md): a source to generate random data for testing.
- [Zero MQ source](./plugin/zmq.md): read data from zero mq.

## Use of sources

The user uses sources by means of streams or tables. The type `TYPE` property needs to be set to the name of the desired source in the stream properties created. The user can also change the behavior of the source during stream creation by configuring various general source attributes, such as the decoding type (default is JSON), etc. For the general properties and creation syntax supported by creating streams, please refer to the [Stream Specification](../streams/overview.md).