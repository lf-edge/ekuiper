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

### Properties

| Property name      | Optional | Description                                                                                               |
|--------------------|----------|-----------------------------------------------------------------------------------------------------------|
| brokers            | false    | The broker address list ,split with ","                                                                   |
| saslAuthType       | true     | The Kafka sasl authType, support none,plain,scram, default none                                           |
| saslUserName       | true     | The sasl user name                                                                                        |
| password           | true     | The sasl password                                                                                         |
| insecureSkipVerify | true     | whether to ignore SSL verification                                                                        |
| certificationPath  | true     | Kafka client ssl verification Cert file path                                                              |
| privateKeyPath     | true     | Key file path for Kafka client SSL verification                                                           |
| rootCaPath         | true     | Kafka client ssl verified CA certificate file path                                                        |
| certficationRaw    | true     | Kafka client ssl verified Cert base64 encoded original text, use `certificationPath` first if both defined |
| privateKeyRaw      | true     | Kafka client ssl verified Key base64 encoded original text, use `privateKeyPath` first if both defined    |
| rootCARaw          | true     | Kafka client ssl verified CA base64 encoded original text, use `rootCaPath` first if both defined         |
| maxBytes           | true     | The maximum number of bytes that a single Kafka message batch can carry, the default is 1MB               |
| groupID            | true     | The group ID used by eKuiper when consuming kafka messages. |
| partition | true     | The partition specified when eKuiper consumes kafka messages |
