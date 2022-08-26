# HTTP push source 

eKuiper provides built-in HTTP source stream, which serves as an HTTP server and can receive the message from HTTP client. There will be a single global HTTP server for all HTTP push sources. Each source can have its own endpoint so that multiple endpoints are supported.

## Configurations

There are two kinds of configurations: global server configuration and the source configuration.

### Server Configuration

The server configuration is in the `source` section in `etc/kuiper.yaml`.

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

User can specify the following properties:

- httpServerIp: the ip to bind the http data server.
- httpServerPort: the port to bind the http data server.
- httpServerTls: the configuration of the http TLS.

The global server will start once any rules needs a httppush source starts. It will shut down once all referred rules are closed.

### Source Configuration

Each stream can configure its url endpoint and http method. The endpoint property is mapped to the `datasource` property in create stream statement.

- Example: Bind the source to `/api/data` endpoint. Thus, with the default server configuration, it will listen on `http://localhost:10081/api/data`.

```sql
CREATE STREAM httpDemo() WITH (DATASOURCE="/api/data", FORMAT="json", TYPE="httppush")
```

The configuration file of HTTP push source is at `etc/sources/httppush.yaml`. Right now, only one property `method` is allowed to configure the http method to listen on.

```yaml
#Global httppush configurations
default:
  # the request method to listen on
  method: "POST"
    
#Override the global configurations
application_conf: #Conf_key
  server: "PUT"
```
