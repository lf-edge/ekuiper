# Kafka Sink

The sink will publish the result into a Kafka .

## Compile & deploy plugin

### build in shell

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/kafka.so extensions/sinks/kafka/kafka.go
# zip kafka.zip plugins/sinks/kafka.so
# cp kafka.zip /root/tomcat_path/webapps/ROOT/
# bin/kuiper create plugin sink kafka -f /tmp/kafkaPlugin.txt
# bin/kuiper create rule kafka -f /tmp/kafkaRule.txt
```

### build with image

```shell
docker build -t demo/plugins:v1 -f build/plugins/Dockerfile .
docker run demo/plugins:v1
docker cp  90eae15a7245:/workspace/_plugins/debian/sinks /tmp
```

Dockerfile like this：

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

add this in Makefile：

```dockerfile
PLUGINS_CUSTOM := sinks/kafka

.PHONY: plugins_c $(PLUGINS_CUSTOM)
plugins_c: $(PLUGINS_CUSTOM)

$(PLUGINS_CUSTOM): PLUGIN_TYPE = $(word 1, $(subst /, , $@))
$(PLUGINS_CUSTOM): PLUGIN_NAME = $(word 2, $(subst /, , $@))
$(PLUGINS_CUSTOM):
  @$(CURDIR)/build-plugins.sh $(PLUGIN_TYPE) $(PLUGIN_NAME)
```

Restart the eKuiper server to activate the plugin.

## Properties

| Property name | Optional | Description                                       |
|---------------|----------|---------------------------------------------------|
| brokers       | false    | The broker address list ,split with ","           |
| topic         | false    | The topic of the Kafka                            |
| saslAuthType  | false    | The Kafka sasl authType, support none,plain,scram |
| saslUserName  | true     | The sasl user name                                |
| saslPassword  | true     | The sasl password                                 |

Other common sink properties are supported. Please refer to the [sink common properties](../overview.md#common-properties) for more information.

## Sample usage

Below is a sample for selecting temperature great than 50 degree, and some profiles only for your reference.

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

## Notice

If ekuiper and kafka are deployed in the same container network through docker compose, you can configure the brokers address through the kafka hostname in ekuiper.
But kafka needs special attention `` KAFKA_CFG_ADVERTISED_LISTENERS `` needs to be configured as the host IP address, as shown below

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
