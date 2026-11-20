# RabbitMQ Source

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>

The source consumes the messages of a [RabbitMQ](https://www.rabbitmq.com/) queue with the AMQP 0-9-1 protocol.

## Compile the plugin

The source is built into the full version of eKuiper. To use it as a plugin, run the below command in the eKuiper
source code root path:

```shell
go build -trimpath --buildmode=plugin -o plugins/sources/Rabbitmq.so extensions/sources/rabbitmq/rabbitmq.go
```

Then copy `plugins/sources/Rabbitmq.so` to the `plugins/sources` folder of the eKuiper installation and restart the
eKuiper server to activate the plugin.

## Configuration

The configuration for this source is `$ekuiper/etc/sources/rabbitmq.yaml`. The format is as below:

```yaml
default:
  server: "amqp://127.0.0.1:5672/"
  prefetchCount: 0
  autoAck: false
```

You can check the connectivity of the corresponding source endpoint in advance through the API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check).
If the `datasource` property is set, the check also verifies that the queue exists.

### Properties

| Property name      | Optional | Description                                                                                                                                                                                                         |
|--------------------|----------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| server             | false    | The [AMQP URI](https://www.rabbitmq.com/docs/uri-spec) of the RabbitMQ server, such as `amqp://127.0.0.1:5672/` or `amqps://127.0.0.1:5671/vhost`. The virtual host is the path of the URI. Use `amqps://` to connect with TLS. |
| username           | true     | The username to connect to the server. It overrides the user in the server URI. Use it with `password` instead of putting the credentials in the URI.                                                            |
| password           | true     | The password to connect to the server.                                                                                                                                                                              |
| prefetchCount      | true     | The maximum number of unacknowledged messages the server delivers to eKuiper, in range 0-65535. The default value 0 means no limit.                                                                                 |
| autoAck            | true     | Whether the server considers a message acknowledged once it is delivered. The default value is `false`, which means eKuiper acknowledges each message after receiving it.                                          |
| consumerTag        | true     | The consumer tag. If it is empty, the server generates one.                                                                                                                                                         |
| insecureSkipVerify | true     | Whether to skip the verification of the server certificate. It can only be used with `amqps://`.                                                                                                                   |
| certificationPath  | true     | The client certificate file path for TLS. It can only be used with `amqps://`.                                                                                                                                      |
| privateKeyPath     | true     | The client private key file path for TLS. It can only be used with `amqps://`.                                                                                                                                      |
| rootCaPath         | true     | The CA certificate file path to verify the server. It can only be used with `amqps://`.                                                                                                                            |
| certificationRaw   | true     | The base64 encoded client certificate. `certificationPath` takes precedence if both are defined.                                                                                                                   |
| privateKeyRaw      | true     | The base64 encoded client private key. `privateKeyPath` takes precedence if both are defined.                                                                                                                      |
| rootCARaw          | true     | The base64 encoded CA certificate. `rootCaPath` takes precedence if both are defined.                                                                                                                              |

The queue must exist before the rule starts. eKuiper does not declare queues or bindings. Use the RabbitMQ management
tools to create the queue and bind it to the exchanges that the producers publish to.

Each rule that uses the stream creates a consumer of the queue. RabbitMQ distributes the messages of a queue among its
consumers, so the rules do not receive the same messages. To process every message by multiple rules, create the stream
as a [shared stream](../../streams/overview.md#share-source-instance-across-rules) with `SHARED="true"`.

### Connection recovery

If the server is unavailable when the rule starts, or the connection is lost while the rule is running, the rule keeps
running and the source reconnects with an exponential backoff of up to 10 seconds. The connection status and the
error are reported in the rule status. The source also reconnects if the server cancels the consumer, for example,
because the queue is deleted.

When `autoAck` is `false`, a message is acknowledged after it is received by the rule. The messages that are delivered
but not acknowledged when the connection is lost are delivered again by the server, with the `redelivered` metadata
set to `true`.

## Metadata

The source adds the properties of each received message into the message metadata (`meta`). The available keys are:

| Key             | Type   | Description                                                                                                               |
|-----------------|--------|---------------------------------------------------------------------------------------------------------------------------|
| exchange        | string | The exchange that the message was published to. It is empty for the default exchange                                     |
| routingKey      | string | The routing key of the message                                                                                            |
| redelivered     | bool   | Whether the message has been delivered before                                                                             |
| contentType     | string | The content type of the message. It is absent if not set                                                                  |
| contentEncoding | string | The content encoding of the message. It is absent if not set                                                              |
| correlationId   | string | The correlation id of the message. It is absent if not set                                                                |
| replyTo         | string | The reply-to address of the message. It is absent if not set                                                              |
| messageId       | string | The id of the message. It is absent if not set                                                                            |
| type            | string | The type of the message. It is absent if not set                                                                          |
| appId           | string | The id of the application that published the message. It is absent if not set                                            |
| timestamp       | int64  | The timestamp of the message in milliseconds. It is absent if not set                                                     |
| headers         | map    | The message headers. Integers are converted to int64, byte arrays to strings and timestamps to milliseconds. It is absent if the message has no headers |

Example:

```sql
SELECT temperature, meta(routingKey) AS routingKey, meta(headers)->deviceId AS deviceId FROM demo
```

## Usage

Create a stream to consume the queue `sensor_data`:

```sql
CREATE STREAM demo (temperature float, humidity bigint) WITH (DATASOURCE="sensor_data", FORMAT="json", TYPE="rabbitmq", CONF_KEY="default")
```
