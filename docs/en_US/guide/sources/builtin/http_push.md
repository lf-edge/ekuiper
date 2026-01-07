# HTTP Push Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

In IoT ecosystems, devices often need to transmit data to processing platforms. The HTTP Push connector in eKuiper allows devices to send their data directly to eKuiper for real-time processing. With eKuiper acting as an endpoint, devices can send data using standard HTTP methods, making integration seamless and straightforward.

When configured as an HTTP Push source, eKuiper exposes an HTTP endpoint so devices can push their data. Once the data is received, eKuiper processes it according to the defined rules and streams.

The connector in eKuiper can be configured with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API](../../../api/restapi/configKey.md), or configuration file. This section focuses on configuring eKuiper connectors with the configuration file.

## Server Configuration

To set up eKuiper as an HTTP endpoint, configure the server settings in `etc/sources/httppush.yaml`.

```yaml
source:
  ## Configurations for the global http data server for httppush source
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

The global server initializes when any rule requiring an HTTP Push source is activated. It terminates once all associated rules are closed.

## Source Configuration

Each [stream](../../streams/overview.md) can have its own unique configuration, allowing it to define URL endpoints and HTTP methods. This flexibility ensures that different streams can handle different types of data and respond to different endpoints as needed.

The HTTP Push source configuration file is located at `etc/sources/httppush.yaml`. The configuration items in the `default` section provide a set of default settings, which you can override as needed.

See below for a demo configuration with the global configuration and a customized `application_conf` section.

```yaml
#Global httppush configurations
default:
  # the request method to listen on
  method: "POST"

#Override the global configurations
application_conf: #Conf_key
  server: "PUT"
```

::: tip

Note: Currently, only the `method` property is available for configuring the HTTP method to listen to.

:::

## Create a Stream Source

Once you've set up your streams with their respective configurations, you can integrate them with eKuiper rules to process and act on the incoming data.

::: tip

HTTP Push connector can function as a [stream source](../../streams/overview.md) or a [scan table](../../tables/scan.md) source. This section illustrates the integration using the HTTP Push Source connector as a stream source example.

:::

You can define the HTTP Push source as the data source either by REST API or CLI tool.

### Use REST API

The REST API offers a programmatic way to interact with eKuiper, perfect for those looking to automate tasks or integrate eKuiper operations into other systems.

Example:

```sql
CREATE STREAM httpDemo() WITH (FORMAT="json", TYPE="httppush")
```

**Create with Custom Configuration**

You can use the `endpoint` property corresponds to the `datasource` property in the stream creation statement.

Example

```sql
CREATE STREAM httpDemo() WITH (DATASOURCE="/api/data", FORMAT="json", TYPE="httppush")
```

In this example, we bind the source to `/api/data` endpoint. Thus, with the default server configuration, it will listen on `http://localhost:10081/api/data`.

More details can be found at [Streams Management with REST API](../../../api/restapi/streams.md).

### Use CLI

For those who prefer a hands-on approach, the Command Line Interface (CLI) provides direct access to eKuiper's operations.

1. Navigate to the eKuiper binary directory:

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. Use the `create` command to create a rule, specifying the HTTP Push connector as its source, for example:

   ```bash
   bin/kuiper CREATE STREAM demo'() with(format="json", datasource="/api/data type="httppush")'
   ```

More details can be found at [Streams Management with CLI](../../../api/cli/streams.md).
