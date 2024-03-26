# Neuron Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

eKuiper's Neuron connector seamlessly integrates with Neuron instances, allowing for efficient data ingestion and output. While it can function as both a source and a [sink connector](../../sinks/builtin/neuron.md), this section focuses on using it as a source connector.

The Neuron source connector is designed to consume events from Neuron instances. One of the primary characteristics of this source connector is its binding with the local Neuron instance, ensuring communication through the nanomsg IPC (Inter-Process Communication) protocol without network dependencies.

::: tip Asynchronous Dial Mechanism

The Neuron source connector features an asynchronous dial mechanism, ensuring continuous background connection attempts to Neuron. However, if the Neuron is down, rules using the Neuron sink may not detect the issue immediately. Always monitor the rule's status and message counts during debugging.

:::

In the eKuiper side, all Neuron source and sink instances share the same connection, thus the events consumed are also the same.

You can check the connectivity of the corresponding sink endpoint in advance through the API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check)

## Configurations

The connector in eKuiper can be configured with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API](../../../api/restapi/configKey.md), or configuration file. This section focuses on the configuration file approach.

The default Neuron connector configuration is found at `$ekuiper/etc/sources/neuron.yaml`.

```yaml
default:
  # The nng connection url to connect to the neuron
  url: tcp://127.0.0.1:7081
ipc:
  url: ipc:///tmp/neuron-ekuiper.ipc
```

This demo configuration provides two different ways to connect: a default TCP connection to a local service on port 7081, and an IPC mechanism for local inter-process communication using a file-based socket.

::: tip

The port specified should correspond to the Neuron instance's port. In this example, we use the default Neuron port, 7081. Ensure you adjust this according to your specific configuration.

:::

```yaml
default:
  # The nng connection url to connect to the neuron
  url: tcp://127.0.0.1:7081
ipc:
  url: ipc:///tmp/neuron-ekuiper.ipc
```

This demo configuration provides two different ways to connect: a default TCP connection to a local service on port 7081, and an IPC mechanism for local inter-process communication using a file-based socket.

::: tip

The port specified should correspond to the Neuron instance's port. In this example, we use the default Neuron port, 7081. Ensure you adjust this according to your specific configuration.

:::

## Neuron Event Format

Neuron events typically adopt the following JSON format:

```json
{
  "timestamp": 1646125996000,
  "node_name": "node1",
  "group_name": "group1",
  "values": {
    "tag_name1": 11.22,
    "tag_name2": "string"
  },
  "errors": {
    "tag_name3": 122
  }
}
```

## Create a Stream Source

Having defined the connector, the next phase involves its integration with eKuiper rules.

::: tip

Neuron Source connector can function as a [stream source](../../streams/overview.md) or a [scan table](../../tables/scan.md) source. This section illustrates the integration using the Neuron Source connector as a stream source example.

:::

You can define the Neuron source as the data source either by REST API or CLI tool.

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, perfect for users looking to automate tasks or integrate eKuiper operations into other systems.

Example:

```sql
CREATE STREAM neuron_stream () WITH (FORMAT="json", TYPE="neuron");
```

More details can be found at [Streams Management with REST API](../../../api/restapi/streams.md).

### Use CLI

For users who prefer a hands-on approach, the Command Line Interface (CLI) provides direct access to eKuiper's operations.

1. Navigate to the eKuiper binary directory:

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. Use the `create` command to define a stream for the Neuron source connector:

   ```bash
   ./kuiper create stream neuron_stream ' WITH (FORMAT="json", TYPE="neuron")'
   ```

More details can be found at [Streams Management with CLI](../../../api/cli/streams.md).
