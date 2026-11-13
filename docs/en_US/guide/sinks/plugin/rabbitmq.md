# RabbitMQ Sink

The sink publishes the results to a [RabbitMQ](https://www.rabbitmq.com/) exchange with the AMQP 0-9-1 protocol.

## Compile the plugin

The sink is built into the full version of eKuiper. To use it as a plugin, run the below command in the eKuiper source
code root path:

```shell
go build -trimpath --buildmode=plugin -o plugins/sinks/Rabbitmq.so extensions/sinks/rabbitmq/rabbitmq.go
```

Then copy `plugins/sinks/Rabbitmq.so` to the `plugins/sinks` folder of the eKuiper installation and restart the eKuiper
server to activate the plugin.

## Properties

| Property name      | Optional | Description                                                                                                                                                                                                         |
|--------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| server             | false    | The [AMQP URI](https://www.rabbitmq.com/docs/uri-spec) of the RabbitMQ server, such as `amqp://127.0.0.1:5672/` or `amqps://127.0.0.1:5671/vhost`. The virtual host is the path of the URI. Use `amqps://` to connect with TLS. |
| username           | true     | The username to connect to the server. It overrides the user in the server URI. Use it with `password` instead of putting the credentials in the URI.                                                            |
| password           | true     | The password to connect to the server.                                                                                                                                                                              |
| exchange           | true     | The exchange to publish to. If it is empty, the default exchange is used, which routes the message to the queue whose name is the routing key.                                                                    |
| routingKey         | true     | The routing key of the message. It is required when publishing to the default exchange. It supports [dynamic properties](../overview.md#dynamic-properties).                                                     |
| contentType        | true     | The content type of the message, such as `application/json`.                                                                                                                                                        |
| persistent         | true     | Whether to publish persistent messages. Persistent messages that are routed to durable queues survive a server restart. The default value is `false`.                                                            |
| publisherConfirm   | true     | Whether to wait for the server to [confirm](https://www.rabbitmq.com/docs/confirms#publisher-confirms) each message. The default value is `false`.                                                                 |
| headers            | true     | The headers of the message as a map of string values. The values support [dynamic properties](../overview.md#dynamic-properties).                                                                                |
| insecureSkipVerify | true     | Whether to skip the verification of the server certificate. It can only be used with `amqps://`.                                                                                                                   |
| certificationPath  | true     | The client certificate file path for TLS. It can only be used with `amqps://`.                                                                                                                                      |
| privateKeyPath     | true     | The client private key file path for TLS. It can only be used with `amqps://`.                                                                                                                                      |
| rootCaPath         | true     | The CA certificate file path to verify the server. It can only be used with `amqps://`.                                                                                                                            |
| certificationRaw   | true     | The base64 encoded client certificate. `certificationPath` takes precedence if both are defined.                                                                                                                   |
| privateKeyRaw      | true     | The base64 encoded client private key. `privateKeyPath` takes precedence if both are defined.                                                                                                                      |
| rootCARaw          | true     | The base64 encoded CA certificate. `rootCaPath` takes precedence if both are defined.                                                                                                                              |

Other common sink properties are supported. Please refer to the [sink common properties](../overview.md#common-properties) for more information.

The exchange must exist before the rule starts. eKuiper does not declare exchanges, queues or bindings. Each message
carries the time it is published as its timestamp.

You can check the connectivity of the server in advance through the API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check).

## Delivery guarantee

If the connection is lost, the sink reconnects when the next message arrives, with an exponential backoff of up to 10
seconds between the attempts. The connection status and the error are reported in the rule status.

Without `publisherConfirm`, a message is considered sent once it is written to the connection. If the server cannot
handle it, for example, because the exchange does not exist, the message is lost and the error is reported in the rule
status.

With `publisherConfirm` enabled, the sink waits until the server confirms each message. A message that fails to send or
is not confirmed is an IO error, so it is sent again if the [cache](../overview.md#caching) or the resend is
configured. This makes the delivery more reliable at the cost of throughput. Note that the server also confirms the
messages that cannot be routed to any queue.

## Sample usage

Publish the results to the queue `alerts` through the default exchange:

```json
{
  "id": "rabbitmqAlert",
  "sql": "SELECT deviceId, temperature FROM demo WHERE temperature > 30",
  "actions": [
    {
      "rabbitmq": {
        "server": "amqp://127.0.0.1:5672/",
        "username": "guest",
        "password": "guest",
        "routingKey": "alerts",
        "contentType": "application/json",
        "persistent": true,
        "publisherConfirm": true
      }
    }
  ]
}
```

Publish to a topic exchange with a dynamic routing key and header:

```json
{
  "id": "rabbitmqTopic",
  "sql": "SELECT deviceId, temperature FROM demo",
  "actions": [
    {
      "rabbitmq": {
        "server": "amqp://127.0.0.1:5672/",
        "exchange": "sensors",
        "routingKey": "sensor.{{.deviceId}}",
        "headers": {
          "deviceId": "{{.deviceId}}"
        },
        "sendSingle": true
      }
    }
  ]
}
```
