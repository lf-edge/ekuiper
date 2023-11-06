# Websocket Source Connector

<span style="background:green;color:white;">stream source</span>

eKuiper has built-in support for Websocket data sources. Through the Websocket data source connector, eKuiper can obtain data from external Websocket clients.

## Server Configuration

To set up eKuiper as an Websocket endpoint, configure the server settings in `etc/sources/websocket.yaml`.

```yaml
source:
  ## Configurations for the global websocket server for websocket source
  # HTTP data service ip
  httpServerIp: 0.0.0.0
  # HTTP data service port
  httpServerPort: 10081
  # httpServerTls:
  #    certfile: /var/https-server.crt
  #    keyfile: /var/https-server.key
```

Users can specify the following properties:

- `httpServerIp`: IP to bind the HTTP data server.
- `httpServerPort`: Port to bind the HTTP data server.
- `httpServerTls`: Configuration of the HTTP TLS.

The global server initializes when any rule requiring an Websocket source is activated. It terminates once all associated rules are closed.

## Create a Stream Source

Once you've set up your streams with their respective configurations, you can integrate them with eKuiper rules to process and act on the incoming data.

::: tip

Websocket connector can function as a [stream source](../../streams/overview.md). This section illustrates the integration using the Websocket Source connector as a stream source example.

:::

You can define the Websocket source as the data source either by REST API or CLI tool.

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, perfect for those looking to automate tasks or integrate eKuiper operations into other systems.

Example:

```sql
CREATE STREAM websocketDemo() WITH (FORMAT="json", TYPE="websocket")
```

**Create with Custom Configuration**

You can use the the `endpoint` property corresponds to the `datasource` property in the stream creation statement.

Example

```sql
CREATE STREAM websocketDemo() WITH (DATASOURCE="/api/data", FORMAT="json", TYPE="websocket")
```

In this example, we bind the source to `/api/data` endpoint. Thus, with the default server configuration, it will listen on `http://localhost:10081/api/data`.

More details can be found at [Streams Management with REST API](../../../api/restapi/streams.md).

### Use CLI

For those who prefer a hands-on approach, the Command Line Interface (CLI) provides direct access to eKuiper's operations.

1. Navigate to the eKuiper binary directory:

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. Use the `create` command to create a rule, specifying the Websocket connector as its source, for example:

   ```bash
   bin/kuiper CREATE STREAM demo'() with(format="json", datasource="/api/data", type="websocket")'
   ```
More details can be found at [Streams Management with CLI](../../../api/cli/streams.md).
