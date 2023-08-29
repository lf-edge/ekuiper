# Neuron Source Connector

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper's Neuron connector seamlessly integrates with local Neuron instances, allowing for efficient data ingestion and output. While it can function both as a source and a [sink connector](../../sinks/builtin/neuron.md), this section delves into its capabilities as a source connector.

The Neuron source connector is designed to consume events from a local Neuron instance. One of the primary characteristics of this source connector is its binding with the local Neuron instance, ensuring communication through the nanomsg IPC (Inter-Process Communication) protocol without network dependencies.

::: tip Asynchronous Dial Mechanism

The Neuron source connector features an asynchronous dial mechanism, ensuring continuous background connection attempts to Neuron. However, if Neuron is down, a rule using the Neuron sink might not immediately detect the problem. Always monitor the rule's status and message counts during debugging.

:::

In the eKuiper side, all neuron source and sink instances share the same connection, thus the events consumed are also the same. 

## Configure Neuron Connector

The connector in eKuiper can be configured with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API,](../../../api/restapi/configKey.md) or configuration file. This section focuses on the configuration file approach.

The default Neuron connector configuration is found at `$ekuiper/etc/sources/neuron.yaml`. 

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



## Integrate Neuron Source with eKuiper Rules

Having defined the connector, the next phase involves its integration with eKuiper rules.

Neuron Source connector can function as a [stream source](../../streams/overview.md) or a [scan table](../../tables/scan.md) source. This section illustrates the integration using the Neuron Source connector as a stream source example.

:::

You can define the MQTT source as the data source either by REST API or CLI tool. 

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, perfect for users looking to automate tasks or integrate eKuiper operations into other systems.

Example: 

```json
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

There is no configuration properties. An example of creating neuron source:

```text
CREATE STREAM table1 () WITH (FORMAT="json", TYPE="neuron");
```
