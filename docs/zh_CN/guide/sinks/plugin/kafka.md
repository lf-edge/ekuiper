# Kafka 目标（Sink）

该插件将分析结果发送到 Kafka 中。

## 编译插件&创建插件

### 本地构建

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/kafka.so extensions/sinks/kafka/kafka.go
# zip kafka.zip plugins/sinks/kafka.so
# cp kafka.zip /root/tomcat_path/webapps/ROOT/
# bin/kuiper create plugin sink kafka -f /tmp/kafkaPlugin.txt
# bin/kuiper create rule kafka -f /tmp/kafkaRule.txt
```

### 镜像构建

```shell
docker build -t demo/plugins:v1 -f build/plugins/Dockerfile .
docker run demo/plugins:v1
docker cp  90eae15a7245:/workspace/_plugins/debian/sinks /tmp
```

Dockerfile 如下所示：

```dockerfile
## plase check go version that kuiper used
ARG GO_VERSION=1.18.5
FROM ghcr.io/lf-edge/ekuiper/base:$GO_VERSION-debian AS builder
WORKDIR /workspace
ADD . /workspace/
RUN go env -w GOPROXY=https://goproxy.cn,direct
RUN make plugins_c
CMD ["sleep","3600"]
```

在 Makefile 中添加：

```dockerfile
PLUGINS_CUSTOM := sinks/kafka

.PHONY: plugins_c $(PLUGINS_CUSTOM)
plugins_c: $(PLUGINS_CUSTOM)

$(PLUGINS_CUSTOM): PLUGIN_TYPE = $(word 1, $(subst /, , $@))
$(PLUGINS_CUSTOM): PLUGIN_NAME = $(word 2, $(subst /, , $@))
$(PLUGINS_CUSTOM):
  @$(CURDIR)/build-plugins.sh $(PLUGIN_TYPE) $(PLUGIN_NAME)
```

重新启动 eKuiper 服务器以激活插件。

## 属性

| 属性名称              | 是否可选 | 说明                             |
|-------------------|------|--------------------------------|
| brokers           | 否    | broker地址列表 ,用 "," 分割           |
| topic             | 否    | kafka 主题                       |
| saslAuthType      | 否    | sasl 认证类型 , 支持none，plain，scram |
| saslUserName      | 是    | sasl 用户名                       |
| saslPassword      | 是    | sasl 密码                        |

其他通用的 sink 属性也支持，请参阅[公共属性](../overview.md#公共属性)。

## 示例用法

下面是选择温度大于50度的样本规则，和一些配置文件仅供参考。

### /tmp/kafkaRule.txt

```json
{
  "id": "kafka",
  "sql": "SELECT * from  demo_stream where temperature > 50",
  "actions": [
    {
      "log": {}
    },
    {
      "kafka":{
        "brokers": "127.0.0.1:9092,127.0.0.2:9092",
        "topic": "test_topic",
        "saslAuthType": "none"
      }
    }
  ]
}
```

### /tmp/kafkaPlugin.txt

```json
{
  "file":"http://localhost:8080/kafka.zip"
}
```

## 注意事项

如果通过 docker compose 将 ekuiper 与 kafka 部署在同一容器网络中，可在 ekuiper 中通过 kafka 主机名配置 brokers 地址。
但是 kafka 需要特别注意 `` KAFKA_CFG_ADVERTISED_LISTENERS `` 需要配置为主机 IP 地址, 如下所示

```yaml
    zookeeper:
     image: docker.io/bitnami/zookeeper:3.8
     hostname: zookeeper
     container_name: zookeeper
     ports:
      - "2181:2181"
     volumes:
      - "zookeeper_data:/bitnami"
     environment:
       - ALLOW_ANONYMOUS_LOGIN=yes
    kafka:
     image: docker.io/bitnami/kafka:3.4
     hostname: kafka
     container_name: kafka
     ports:
      - "9092:9092"
     volumes:
      - "kafka_data:/bitnami"
     environment:
      - KAFKA_CFG_ZOOKEEPER_CONNECT=zookeeper:2181
      - ALLOW_PLAINTEXT_LISTENER=yes
      - KAFKA_CFG_LISTENERS=PLAINTEXT://:9092
      - KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://122.9.166.75:9092
     depends_on:
      - zookeeper

```
