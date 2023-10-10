## Redis Source Connector

<span style="background:green;color:white">lookup table source</span>

eKuiper provides built-in support for looking up data in Redis. The Redis Source Connector allows eKuiper to ingest data from a Redis instance, enabling real-time stream processing based on Redis data. With its in-memory data structure store capabilities, Redis is a vital tool in many application data architectures. Integrating it with eKuiper stream processing expands the realm of possibilities for real-time analytics and decision-making.

::: tip

Currently, the Redis source can only be used as a [lookup table](../../tables/lookup.md),
while the [RedisSub source](./redisSub.md) can be used as both a streaming and scanning table data source,

:::

## Configurations

Before using the Redis Source Connector, it's essential to configure the connection settings and other relevant parameters. Here are the available configuration options:

The configuration file for the Redis source is located at */etc/sources/redis.yaml*.

```yaml
default:
  # the redis host address
  addr: "127.0.0.1:6379"
  # currently supports string and list only
  datatype: "string"
#  username: ""
#  password: ""
```

With this configuration, the table will refer to database 0 in the Redis instance at the address 127.0.0.1:6379.

**Configuration Items**

- **`addr`**: This specifies the address of the Redis server, a string in the format `hostname:port` or `IP_address:port`.
- **`datatype`**: This determines the type of data the connector should expect from the Redis key. Currently only `string` and `list` are supported.
- **`username`**: The username for accessing the Redis server, only needed if authentication is enabled on the server.
- **`password`**: The password for accessing the Redis server, only needed if authentication is enabled on the server.

## Create a Lookup Table Source

To utilize the Redis Source Connector in eKuiper streams, define a stream specifying the Redis source, its configuration, and the data format.

You can define the Redis source as the data source either by REST API or CLI tool.

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, perfect for users looking to automate tasks or integrate eKuiper operations into other systems.

Example:

```sql
create table table1 () WITH (DATASOURCE="0", FORMAT="json", TYPE="redis", KIND="lookup");
```

More details can be found at [Streams Management with REST API](../../../api/restapi/streams.md).

### Use CLI

For users who prefer a hands-on approach, the Command Line Interface (CLI) provides direct access to eKuiper's operations.

1. Navigate to the eKuiper binary directory:

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. Use the `create` command to define a stream for the Redis source connector:

   ```bash
   ./kuiper create stream neuron_stream ' WITH (DATASOURCE="0", FORMAT="json", TYPE="redis", KIND="lookup")'
   ```

More details can be found at [Streams Management with CLI](../../../api/cli/streams.md).
