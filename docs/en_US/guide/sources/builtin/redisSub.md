## RedisSub Source Connector

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper has built-in support for Redis data sources, allowing data querying and channel subscription within Redis.Please note that the RedisSub source can be used as both a streaming and scanning table data source,while the [Redis source](./redis.md) can be used as a [lookup table](../../tables/lookup.md).

## Configurations

Before using the RedisSub Source Connector, it's essential to configure the connection settings and other relevant parameters. Here are the available configuration options:

The configuration file for the RedisSub source is located at */etc/sources/redisSub.yaml*.

```yaml
default:
   address: 127.0.0.1:6379
   username: default
   db: 0
```

**Configuration Items**

- **`address`**：Specifies the address of the Redis server in the format hostname:port or IP_address:port.
- **`username`**：Sets the username for accessing the Redis server. This is only required when the server has authentication enabled.
- **`password`**：Sets the password for accessing the Redis server. This is only required when the server has authentication enabled.
- **`db`**：Selects the Redis database to connect to. The default is 0.
- **`channels`**：Used to specify a list of Redis channels to subscribe to.
- **`decompression`**：Specifies the compression method for decompressing Redis Payload. Supported compression methods include "zlib," "gzip," "flate," and "zstd."

## Create a Stream Source

To utilize the RedisSub source Connector in eKuiper streams, define a stream specifying the RedisSub source, its configuration, and the data format.

You can define the RedisSub source as the data source either by REST API or CLI tool.

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, perfect for users looking to automate tasks or integrate eKuiper operations into other systems.

**Example**

```sql
CREATE STREAM redisSub_stream () WITH (FORMAT="json", TYPE="redisSub");
```

More details can be found at [Streams Management with REST API](../../../api/restapi/streams.md).

### Use CLI

For users who prefer a hands-on approach, the Command Line Interface (CLI) provides direct access to eKuiper's operations.

1. Navigate to the eKuiper binary directory:

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. Use the `create` command to define a stream for the RedisSub source connector:

   ```bash
   ./kuiper create stream redisSub_stream ' WITH (FORMAT="json", TYPE="redisSub")'
   ```

More details can be found at [Streams Management with CLI](../../../api/cli/streams.md).
