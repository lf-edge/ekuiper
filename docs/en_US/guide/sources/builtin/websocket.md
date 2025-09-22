# Websocket Source Connector

<span style="background:green;color:white;">stream source</span>

eKuiper has built-in support for Websocket data sources. Through the Websocket data source connector, eKuiper can obtain data through websocket connection.

When eKuiper uses the websocket data source, eKuiper will get the data from the websocket TextMessage and parse it in the form of json object data.

## eKuiper serve as websocket client

eKuiper can serve as a websocket client, initiate a websocket connection to the remote websocket server, and receive data on the websocket connection as a message source.

When you need eKuiper as a websocket client, you need to specify the server address of the websocket connection in the corresponding confKey, and declare the corresponding url in the dataSource of the stream, as follows:

```yaml
default:
  addr: 127.0.0.1:8080
  scheme: ws
```

```sql
CREATE STREAM demo'() with(CONF_KEY="default", datasource="/api/data", type="websocket")'
```

At this time, eKuiper will act as a websocket client, establish a websocket connection to 127.0.0.1:8080/api/data, and use this connection to receive data as the message source.

You can check the connectivity of the corresponding sink endpoint in advance through the API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check)

## eKuiper serve as websocker server

eKuiper can serve as a websocket server. At this time, the remote websocket client can actively initiate a websocket connection to eKuiper, and eKuiper will receive messages on the websocket connection as the message source.

When you need eKuiper as a websocket server, you need to specify that the websocket server address is empty in the corresponding confKey, and declare the corresponding url in the dataSource of the stream, as follows:

```yaml
default:
  addr: ""
```

```sql
CREATE STREAM demo'() with(CONF_KEY="default", datasource="/api/data", type="websocket")'
```

At this time, eKuiper will serve as the websocket server, use itself as the host, wait for the websocket connection to be established at the URL of /api/data, and use this connection to receive data as the message source.

### Server Configuration

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

You can use the `endpoint` property corresponds to the `datasource` property in the stream creation statement.

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
