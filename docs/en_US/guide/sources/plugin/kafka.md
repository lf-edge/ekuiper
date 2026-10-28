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
| offsetReset | true     | Specifies where to start consuming when there is no committed offset. Supported values are `earliest` and `latest`. If not set, the default depends on the consumption mode: without `groupID`, it starts from the latest offset; with `groupID`, it starts from the earliest offset. |

## Metadata

The source adds the information of each received Kafka message into the message metadata (`meta`). The available keys are:

| Key       | Type                | Description                                                                                                  |
|-----------|---------------------|--------------------------------------------------------------------------------------------------------------|
| topic     | string              | The topic of the message                                                                                     |
| partition | int                 | The partition of the message                                                                                 |
| offset    | int64               | The offset of the message in the partition                                                                   |
| key       | string              | The message key. It is absent if the message has no key                                                      |
| headers   | map                 | The message headers as a map of header key to string value. It is absent if the message has no headers. For duplicated header keys, the last one wins |

As `partition` is a keyword in SQL, quote it with backticks when referring to it.

Example:

```sql
SELECT meta(key) AS key, meta(`partition`) AS `partition`, meta(offset) AS offset, meta(headers)->traceId AS traceId FROM demo
```
