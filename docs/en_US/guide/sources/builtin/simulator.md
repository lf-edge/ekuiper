# Simulator Source Connector

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

Simulator source provides a way to generate data for testing and demo purposes. It can be used to simulate a stream of
data from a device or sensor. User can define the mock data content and the sending interval.

## Configurations

The connector in eKuiper can be configured
with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API](../../../api/restapi/configKey.md),
or configuration file. This section focuses on the configuration file approach.

The default simulator source configuration can be found at `$ekuiper/etc/sources/simulator.yaml`. It defines the mock
data and the interval to generate the data.

```yaml
default:
  data:
    - temperature: 22.5
      humidity: 50
  interval: 10
  loop: true
```

Users can specify the following properties:

- `data`: The mock data to be sent. It is a yaml struct. The key is the field name and the value is the field value. It
  can also be a list of yaml struct. The connector will send the data in the list one by one.
- `interval`: The interval in milliseconds to generate the data.
- `loop`: Whether to loop the data. If set to true, the connector will send the data in the list one by one and then
  start from the beginning again. If set to false, the connector will stop after sending all the data in the list.

## Create a Stream Source

Having defined the connector, the next phase involves its integration with eKuiper rules.

::: tip

Simulator Source connector can function as a [stream source](../../streams/overview.md) or
a [scan table](../../tables/scan.md) source. This section illustrates the integration using the Simulator Source
connector as a stream source example.

:::

You can define the Simulator source as the data source either by REST API or CLI tool.

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, perfect for users looking to automate tasks or
integrate eKuiper operations into other systems.

Example:

```sql
CREATE
STREAM mock_stream () WITH (TYPE="simulator");
```

More details can be found at [Streams Management with REST API](../../../api/restapi/streams.md).

### Use CLI

For users who prefer a hands-on approach, the Command Line Interface (CLI) provides direct access to eKuiper's
operations.

1. Navigate to the eKuiper binary directory:

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. Use the `create` command to define a stream for the Neuron source connector:

   ```bash
   ./kuiper create stream mock_stream ' WITH (TYPE="simulator")'
   ```

More details can be found at [Streams Management with CLI](../../../api/cli/streams.md).
