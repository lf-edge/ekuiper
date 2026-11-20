# RabbitMQ 数据源

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>

该源通过 AMQP 0-9-1 协议消费 [RabbitMQ](https://www.rabbitmq.com/) 队列中的消息。

## 编译插件

该源已内置于 eKuiper 的 full 版本中。若要以插件形式使用，请在 eKuiper 源代码根目录下执行以下命令：

```shell
go build -trimpath --buildmode=plugin -o plugins/sources/Rabbitmq.so extensions/sources/rabbitmq/rabbitmq.go
```

然后将 `plugins/sources/Rabbitmq.so` 复制到 eKuiper 安装目录的 `plugins/sources` 文件夹中，并重启 eKuiper 服务器以激活插件。

## 配置

该源的配置文件位于 `$ekuiper/etc/sources/rabbitmq.yaml`，格式如下：

```yaml
default:
  server: "amqp://127.0.0.1:5672/"
  prefetchCount: 0
  autoAck: false
```

你可以通过 api 的方式提前检查对应源端点的连通性：[连通性检查](../../../api/restapi/connection.md#连通性检查)。若设置了 `datasource`
属性，还会检查该队列是否存在。

### 属性

| 属性名称               | 是否可选  | 描述                                                                                                                                                       |
|--------------------|-------|----------------------------------------------------------------------------------------------------------------------------------------------------------|
| server             | false | RabbitMQ 服务器的 [AMQP URI](https://www.rabbitmq.com/docs/uri-spec)，例如 `amqp://127.0.0.1:5672/` 或 `amqps://127.0.0.1:5671/vhost`。URI 的路径为虚拟主机。使用 `amqps://` 以通过 TLS 连接。 |
| username           | true  | 连接服务器的用户名，会覆盖服务器 URI 中的用户。建议与 `password` 一起使用，而不是将凭证写在 URI 中。                                                                                             |
| password           | true  | 连接服务器的密码。                                                                                                                                                |
| prefetchCount      | true  | 服务器向 eKuiper 推送的未确认消息的最大数量，取值范围为 0-65535。默认值 0 表示不限制。                                                                                                  |
| autoAck            | true  | 服务器是否在投递消息后即认为消息已确认。默认值为 `false`，即 eKuiper 在接收每条消息后确认该消息。                                                                                              |
| consumerTag        | true  | 消费者标签。若为空，由服务器生成。                                                                                                                                        |
| insecureSkipVerify | true  | 是否跳过服务器证书的验证。只能与 `amqps://` 一起使用。                                                                                                                       |
| certificationPath  | true  | TLS 客户端证书文件路径。只能与 `amqps://` 一起使用。                                                                                                                       |
| privateKeyPath     | true  | TLS 客户端私钥文件路径。只能与 `amqps://` 一起使用。                                                                                                                       |
| rootCaPath         | true  | 用于验证服务器的 CA 证书文件路径。只能与 `amqps://` 一起使用。                                                                                                                  |
| certificationRaw   | true  | base64 编码的客户端证书。若同时定义，优先使用 `certificationPath`。                                                                                                         |
| privateKeyRaw      | true  | base64 编码的客户端私钥。若同时定义，优先使用 `privateKeyPath`。                                                                                                            |
| rootCARaw          | true  | base64 编码的 CA 证书。若同时定义，优先使用 `rootCaPath`。                                                                                                               |

队列必须在规则启动前存在。eKuiper 不会声明队列或绑定。请使用 RabbitMQ 管理工具创建队列，并将其绑定到生产者发布消息的交换机。

每个使用该流的规则都会创建一个队列的消费者。RabbitMQ 会在队列的多个消费者之间分发消息，因此各规则不会收到相同的消息。若要让多个规则处理每一条消息，请使用 `SHARED="true"` 将该流创建为[共享流](../../streams/overview.md#共享源实例)。

### 连接恢复

如果规则启动时服务器不可用，或者规则运行期间连接断开，规则会继续运行，并且该源会以最长 10 秒的指数退避间隔重新连接。连接状态和错误会在规则状态中报告。如果服务器取消了消费者，例如因为队列被删除，该源也会重新连接。

当 `autoAck` 为 `false` 时，消息在被规则接收后确认。连接断开时已投递但尚未确认的消息会被服务器重新投递，其 `redelivered` 元数据为 `true`。

## 元数据

该源会将接收到的每条消息的属性添加到消息元数据（`meta`）中。可用的键如下：

| 键               | 类型     | 描述                                                         |
|-----------------|--------|------------------------------------------------------------|
| exchange        | string | 消息发布到的交换机。对于默认交换机，该值为空                                     |
| routingKey      | string | 消息的路由键                                                     |
| redelivered     | bool   | 消息是否曾被投递过                                                  |
| contentType     | string | 消息的内容类型。未设置时不存在                                            |
| contentEncoding | string | 消息的内容编码。未设置时不存在                                            |
| correlationId   | string | 消息的关联 ID。未设置时不存在                                           |
| replyTo         | string | 消息的回复地址。未设置时不存在                                            |
| messageId       | string | 消息 ID。未设置时不存在                                              |
| type            | string | 消息类型。未设置时不存在                                               |
| appId           | string | 发布消息的应用 ID。未设置时不存在                                         |
| timestamp       | int64  | 消息的时间戳，单位为毫秒。未设置时不存在                                       |
| headers         | map    | 消息头。整数转换为 int64，字节数组转换为字符串，时间戳转换为毫秒。消息没有消息头时不存在 |

示例：

```sql
SELECT temperature, meta(routingKey) AS routingKey, meta(headers)->deviceId AS deviceId FROM demo
```

## 使用

创建一个消费队列 `sensor_data` 的流：

```sql
CREATE STREAM demo (temperature float, humidity bigint) WITH (DATASOURCE="sensor_data", FORMAT="json", TYPE="rabbitmq", CONF_KEY="default")
```
