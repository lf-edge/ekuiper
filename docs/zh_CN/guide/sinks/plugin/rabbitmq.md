# RabbitMQ Sink

该动作通过 AMQP 0-9-1 协议将结果发布到 [RabbitMQ](https://www.rabbitmq.com/) 交换机。

## 编译插件

该动作已内置于 eKuiper 的 full 版本中。若要以插件形式使用，请在 eKuiper 源代码根目录下执行以下命令：

```shell
go build -trimpath --buildmode=plugin -o plugins/sinks/Rabbitmq.so extensions/sinks/rabbitmq/rabbitmq.go
```

然后将 `plugins/sinks/Rabbitmq.so` 复制到 eKuiper 安装目录的 `plugins/sinks` 文件夹中，并重启 eKuiper 服务器以激活插件。

## 属性

| 属性名称               | 是否可选  | 描述                                                                                                                                                       |
|--------------------|-------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| server             | false | RabbitMQ 服务器的 [AMQP URI](https://www.rabbitmq.com/docs/uri-spec)，例如 `amqp://127.0.0.1:5672/` 或 `amqps://127.0.0.1:5671/vhost`。URI 的路径为虚拟主机。使用 `amqps://` 以通过 TLS 连接。 |
| username           | true  | 连接服务器的用户名，会覆盖服务器 URI 中的用户。建议与 `password` 一起使用，而不是将凭证写在 URI 中。                                                                                             |
| password           | true  | 连接服务器的密码。                                                                                                                                                |
| exchange           | true  | 要发布到的交换机。若为空，则使用默认交换机，消息将被路由到与路由键同名的队列。                                                                                                                  |
| routingKey         | true  | 消息的路由键。发布到默认交换机时必填。支持[动态属性](../overview.md#动态属性)。                                                                                                        |
| contentType        | true  | 消息的内容类型，例如 `application/json`。                                                                                                                          |
| persistent         | true  | 是否发布持久化消息。持久化消息被路由到持久化队列后，在服务器重启后仍然保留。默认值为 `false`。                                                                                                     |
| publisherConfirm   | true  | 是否等待服务器[确认](https://www.rabbitmq.com/docs/confirms#publisher-confirms)每条消息。默认值为 `false`。                                                               |
| headers            | true  | 消息头，值为字符串的 map。值支持[动态属性](../overview.md#动态属性)。                                                                                                          |
| insecureSkipVerify | true  | 是否跳过服务器证书的验证。只能与 `amqps://` 一起使用。                                                                                                                       |
| certificationPath  | true  | TLS 客户端证书文件路径。只能与 `amqps://` 一起使用。                                                                                                                       |
| privateKeyPath     | true  | TLS 客户端私钥文件路径。只能与 `amqps://` 一起使用。                                                                                                                       |
| rootCaPath         | true  | 用于验证服务器的 CA 证书文件路径。只能与 `amqps://` 一起使用。                                                                                                                  |
| certificationRaw   | true  | base64 编码的客户端证书。若同时定义，优先使用 `certificationPath`。                                                                                                         |
| privateKeyRaw      | true  | base64 编码的客户端私钥。若同时定义，优先使用 `privateKeyPath`。                                                                                                            |
| rootCARaw          | true  | base64 编码的 CA 证书。若同时定义，优先使用 `rootCaPath`。                                                                                                               |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

交换机必须在规则启动前存在。eKuiper 不会声明交换机、队列或绑定。每条消息以其发布时间作为时间戳。

你可以通过 api 的方式提前检查服务器的连通性：[连通性检查](../../../api/restapi/connection.md#连通性检查)。

## 投递保证

如果连接断开，该动作会在下一条消息到达时重新连接，两次尝试之间的指数退避间隔最长为 10 秒。连接状态和错误会在规则状态中报告。

未开启 `publisherConfirm` 时，消息写入连接后即被视为已发送。如果服务器无法处理该消息，例如交换机不存在，消息将丢失，并在规则状态中报告错误。

开启 `publisherConfirm` 后，该动作会等待服务器确认每条消息。发送失败或未被确认的消息属于 IO 错误，因此在配置了[缓存](../overview.md#缓存)或重发时会被重新发送。这会以吞吐量为代价提高投递的可靠性。注意，服务器同样会确认无法路由到任何队列的消息。

## 使用样例

通过默认交换机将结果发布到队列 `alerts`：

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

使用动态路由键和消息头发布到主题交换机：

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
