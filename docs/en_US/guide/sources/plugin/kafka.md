# Kafka Source

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>

The source will query the Kafka periodically to get data stream.

## Default build command

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sources/kafka.so extensions/sources/kafka/kafka.go
# cp plugins/sources/kafka.so $eKuiper_install/plugins/sources
```

Restart the eKuiper server to activate the plugin.

## Configuration

The configuration for this source is `$ekuiper/etc/sources/kafka.yaml`. The format is as below:

```yaml
default:
  brokers: "127.0.0.1:9091,127.0.0.1:9092"
  groupID: ""
  partition: 0
  maxBytes: 1000000
```

You can check the connectivity of the corresponding sink endpoint in advance through the API: [Connectivity Check](../../../api/restapi/connection.md#connectivity-check)

### Global configurations

User can specify the global Kafka source settings here. The configuration items specified in `default` section will be taken as default settings for the source when running this source.

### brokers

Kafka message source address, the address is separated by `,`.

### groupID

The group ID used by eKuiper when consuming kafka messages.

### partition

The partition specified when eKuiper consumes kafka messages

### maxBytes

The maximum number of bytes that a single Kafka message batch can carry, the default is 1MB
