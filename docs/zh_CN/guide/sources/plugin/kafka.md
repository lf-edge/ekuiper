# Kafka 源

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>

源将订阅 Kafka 消息源从而获取信息

## 默认构建命令

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sources/kafka.so extensions/sources/kafka/kafka.go
# cp plugins/sources/kafka.so $eKuiper_install/plugins/sources
```

重启 eKuiper 服务器以激活插件

## 配置

这个数据流的配置文件位于 `$ekuiper/etc/sources/kafka.yaml`. 格式如下:

```yaml
default:
  brokers: "127.0.0.1:9091,127.0.0.1:9092"
  groupID: ""
  partition: 0
  maxBytes: 1000000
```

你可以通过 api 的方式提前检查对应 sink 端点的连通性: [连通性检查](../../../api/restapi/connection.md#连通性检查)

### 属性

| 属性名称               | 是否可选 | 说明                                                                             |
|--------------------|------|--------------------------------------------------------------------------------|
| brokers            | 否    | broker地址列表 ,用 "," 分割                                                           |
| topic              | 否    | kafka 主题                                                                       |
| saslAuthType       | 是    | sasl 认证类型 , 支持none，plain，scram, 默认为 none                                       |
| saslUserName       | 是    | sasl 用户名                                                                       |
| password           | 是    | sasl 密码                                                                        |
| insecureSkipVerify | 是    | 是否忽略 SSL 验证                                                                    |
| certificationPath  | 是    | Kafka 客户端 ssl 验证的 crt 文件路径                                                     |
| privateKeyPath     | 是    | Kafka 客户端 ssl 验证的 key 文件路径                                                     |
| rootCaPath         | 是    | Kafka 客户端 ssl 验证的 ca 证书文件路径                                                    |
| certficationRaw    | 是    | Kafka 客户端 ssl 验证，经过 base64 编码过的的 crt 原文,  如果同时定义了 `certificationPath` 将会先用该参数。 |
| privateKeyRaw      | 是    | Kafka 客户端 ssl 验证，经过 base64 编码过的的 key 原文,  如果同时定义了 `privateKeyPath` 将会先用该参数。    |
| rootCARaw          | 是    | Kafka 客户端 ssl 验证，经过 base64 编码过的的 ca 原文,  如果同时定义了 `rootCAPath` 将会先用该参数。         |
| maxBytes           | 是    | 单个 kafka 消息批次最大所能携带的 bytes 数，默认为 1MB                                           |
