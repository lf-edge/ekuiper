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

### 全局配置

用户可以在此处指定全局 kafka 源设置。`default` 部分中指定的配置项将在运行此源时作为源的默认设置。

### brokers

kafka 消息源地址，多个地址以 `,` 分割。

### groupID

eKuiper 消费 kafka 消息时所使用的 group ID。

### partition

eKuiper 消费 kafka 消息时所指定的 partition

### maxBytes

单个 kafka 消息批次最大所能携带的 bytes 数，默认为 1MB
