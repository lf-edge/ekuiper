# Websocket action

The action is used for publishing output message into websocket channel.

## Properties

| Property name  | Optional | Description                                           |
| addr           | false    | The address of the websocket sink server, like: 127.0.0.1:8080 |
| path           | false    | The url path of the websocket sink server, like: /api/data     |
| insecureSkipVerify | true | whether to ignore SSL verification |
| certificationPath  | true | websocket client ssl verification crt file path |
| privateKeyPath     | true | Key file path for websocket client SSL verification |
| rootCaPath         | true | websocket client ssl verified ca certificate file path |

Other common sink properties are supported. Please refer to the [sink common properties](../overview.md#common-properties) for more information.

## eKuiper as websocket client

When the websocket sink defines both addr and path, eKuiper will act as a websocket client to establish a websocket connection to the remote end and push messages through the connection.

## eKuiper as websocket server

When the websocket sink only defines path and addr is empty, eKuiper will serve as the websocket server and wait for the remote websocket connection to be established and push the message through the connection.

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

## Sample usage

The following is an example of publishing compressed data to a websocket server.

```json
{
  "websocket":{
    "address": "127.0.0.1:8080",
    "path": "/api/data"
  }
}
```
