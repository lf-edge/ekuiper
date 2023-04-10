# Sink Connectors

In the eKuiper source code, there are built-in sinks and sinks in extension.

## Built-in Sinks

Users can directly use the built-in sinks in the standard eKuiper instance. The list of built-in sinks are:

- [Mqtt sink](./builtin/mqtt.md): sink to external mqtt broker.
- [Neuron sink](./builtin/neuron.md): sink to the local neuron instance.
- [EdgeX sink](./builtin/edgex.md): sink to EdgeX Foundry. This sink only exist when enabling edgex build tag.
- [Rest sink](./builtin/rest.md): sink to external http server.
- [Redis sink](./builtin/redis.md): sink to redis.
- [File sink](./builtin/file.md): sink to a file.
- [Memory sink](./builtin/memory.md): sink to eKuiper memory topic to form rule pipelines.
- [Log sink](./builtin/log.md): sink to log, usually for debug only.
- [Nop sink](./builtin/nop.md): sink to nowhere. It is used for performance testing now.

## Predefined Sink Plugins

We have developed some official sink plugins. These plugins can be found in eKuiper's source code and users need to build them manually. Please check each sink about how to build and use.

Additionally, these plugins have pre-built binaries for the mainstream cpu architecture such as AMD or ARM. The pre-built plugin hosted in `https://packages.emqx.net/kuiper-plugins/$version/$os/sinks/$type_$arch.zip`. For example, to get tdengine sink for debian amd64, install it from `https://packages.emqx.net/kuiper-plugins/1.4.4/debian/sinks/tdengine_amd64.zip`.

The list of predefined sink plugins:

- [InfluxDB sink](./plugin/influx.md): sink to influx db `v1.x`. 
- [InfluxDBV2 sink](./plugin/influx2.md): sink to influx db `v2.x`.
- [Tdengine sink](./plugin/tdengine.md): sink to tdengine.
- [Image sink](./plugin/image.md): sink to an image file. Only used to handle binary result.
- [Zero MQ sink](./plugin/zmq.md): sink to zero mq.
- [Kafka sink](./plugin/kafka.md): sink to kafka.

## Updatable Sink

By default, sinks append data to the external system. Some external system such as SQL DB is updatable which allows to update or delete data. Similar to lookup source, only a few sinks are "updatable" naturally. The sink must support insert, update and delete. The shipped updatable sinks include:

- Memory sink
- Redis sink
- SQL sink

To activate the update feature, the sink must set the `rowkindField` property to specify which field in the data represents to action to take. In the below example, `rowkindField` is set to `action`. 

```json
{"redis": {
  "addr": "127.0.0.1:6379",
  "dataType": "string",
  "field": "id",
  "rowkindField": "action",
  "sendSingle": true
}}
```

The data ingested must have a field to indicate the update action. In the below example, the `action` field is the action to perform. The actions could be `insert`, `update`, `upsert` and `delete`. The action implementation varies between sinks. Some sinks may perform the same action for insert, upsert and update.

```json
{"action":"update", "id":5, "name":"abc"}
```

This message will update the data of id 5 to the new name.

## Common Properties

Each sink has its own property set based on the common properties.

Each action can define its own properties. There are several common properties:

| property name        | Type & Default Value               | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|----------------------|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| concurrency          | int: 1                             | Specify how many instances of the sink will be run. If the value is bigger than 1, the order of the messages may not be retained.                                                                                                                                                                                                                                                                                                                                           |
| bufferLength         | int: 1024                          | Specify how many messages can be buffered in memory. If the buffered messages exceed the limit, the sink will block message receiving until the buffered messages have been sent out so that the buffered size is less than the limit.                                                                                                                                                                                                                                      |
| runAsync             | bool:false                         | Whether the sink will run asynchronously for better performance. If it is true, the sink result order is not promised.                                                                                                                                                                                                                                                                                                                                                      |
| omitIfEmpty          | bool: false                        | If the configuration item is set to true, when SELECT result is empty, then the result will not feed to sink operator.                                                                                                                                                                                                                                                                                                                                                      |
| sendSingle           | bool: false                        | The output messages are received as an array. This is indicate whether to send the results one by one. If false, the output message will be `{"result":"${the string of received message}"}`. For example, `{"result":"[{\"count\":30},"\"count\":20}]"}`. Otherwise, the result message will be sent one by one with the actual field name. For the same example as above, it will send `{"count":30}`, then send `{"count":20}` to the RESTful endpoint.Default to false. |
| dataTemplate         | string: ""                         | The [golang template](https://golang.org/pkg/text/template) format string to specify the output data format. The input of the template is the sink message which is always an array of map. If no data template is specified, the raw input will be the data. Please check [data template](./data_template.md) for detail.                                                                                                                                                  |
| format               | string: "json"                     | The encode format, could be "json" or "protobuf". For "protobuf" format, "schemaId" is required and the referred schema must be registered.                                                                                                                                                                                                                                                                                                                                 |
| schemaId             | string: ""                         | The schema to be used to encode the result.                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| delimiter            | string: ","                        | Only effective when using `delimited` format, specify the delimiter character, default is commas.                                                                                                                                                                                                                                                                                                                                                                           |
| enableCache          | bool: default to global definition | whether to enable sink cache. cache storage configuration follows the configuration of the metadata store defined in `etc/kuiper.yaml`                                                                                                                                                                                                                                                                                                                                      |
| memoryCacheThreshold | int: default to global definition  | the number of messages to be cached in memory. For performance reasons, the earliest cached messages are stored in memory so that they can be resent immediately upon failure recovery. Data here can be lost due to failures such as power outages.                                                                                                                                                                                                                        |
| maxDiskCache         | int: default to global definition  | The maximum number of messages to be cached on disk. The disk cache is first-in, first-out. If the disk cache is full, the earliest page of information will be loaded into the memory cache, replacing the old memory cache.                                                                                                                                                                                                                                               |
| bufferPageSize       | int: default to global definition  | buffer pages are units of bulk reads/writes to disk to prevent frequent IO. if the pages are not full and eKuiper crashes due to hardware or software errors, the last unwritten pages to disk will be lost.                                                                                                                                                                                                                                                                |
| resendInterval       | int: default to global definition  | The time interval to resend information after failure recovery to prevent message storms.                                                                                                                                                                                                                                                                                                                                                                                   |
| cleanCacheAtStop     | bool: default to global definition | whether to clean all caches when the rule is stopped, to prevent mass resending of expired messages when the rule is restarted. If not set to true, the in-memory cache will be stored to disk once the rule is stopped. Otherwise, the memory and disk rules will be cleared out.                                                                                                                                                                                          |


### Dynamic properties

In the sink, it is common to fetch a property value from the result data to achieve dynamic output. For example, to write data into a dynamic topic of mqtt. The dynamic properties will be parsed as a [data template](./data_template.md). In below example, the sink topic is gotten from the selected topic using data template.

```json
{
  "id": "rule1",
  "sql": "SELECT topic FROM demo",
  "actions": [{
    "mqtt": {
      "sendSingle": true,
      "topic": "prefix/{{.topic}}"
    }
  }]
}
```

In the above example, `sendSingle` property is used, so the sink data is a map by default. If not using `sendSingle`, you can get the topic by index with data template <code v-pre>{{index . 0 "topic"}}</code>.

## Caching

Sinks are used to send processing results to external systems. There are situations where the external system is not available, especially in edge-to-cloud scenarios. For example, in a weak network scenario, the edge-to-cloud network connection may be disconnected and reconnected from time to time. Therefore, sinks provide caching capabilities to temporarily store data in case of recoverable errors and automatically resend the cached data after the error is recovered. Sink's cache can be divided into two levels of storage, namely memory and disk. The user can configure the number of memory cache entries and when the limit is exceeded, the new cache will be stored offline to disk. The cache will be stored in both memory and disk so that the cache capacity becomes larger; it will also continuously detect the failure state and resend without restarting the rule.

The storage location of the offline cache is determined by the storage configuration in `etc/kuiper.yaml`, which defaults to sqlite. If the disk storage is sqlite, all caches will be saved to the `data/cache.db` file. Each sink will have a unique sqlite table to hold the cache. The number of caches is added to the buffer length section of the sink's metrics.

### Flow

Each sink can configure its own caching mechanism. The caching process is the same for each sink. If caching is enabled, all sink's events go through two phases: first, saving all content to the cache; then deleting the cache after receiving an ack.

- Error detection: After a failed send, sink should identify recoverable failures (network, etc.) by returning a specific error type, which will return a failed ack so that the cache can be retained. For successful sends or unrecoverable errors, a successful ack will be sent to delete the cache.
- Cache mechanism: The cache will first be kept in memory. If the memory threshold is exceeded, the later cache will be saved to disk. Once the disk cache exceeds the disk storage threshold, the cache will start to rotate, i.e. the earliest cache in memory will be discarded and the earliest cache on disk will be loaded instead.
- Resend policy: Currently the caching mechanism can only run in the default synchronous mode, where if a message is being sent, it will wait for the result of the send to continue sending the next cached data. Otherwise, when new data arrives, the first data in the cache is sent to detect network conditions. If the send is successful, all caches in memory and on disk are sent in a sequential chain. Chained sends can define a send interval to prevent message storms.

### Configuration

There are two levels of configuration for the Sink cache. A global configuration in `etc/kuiper.yaml` that defines the default behavior of all rules. There is also a rule sink level definition to override the default behavior.

- enableCache: whether to enable sink cache. cache storage configuration follows the configuration of the metadata store defined in `etc/kuiper.yaml`.
- memoryCacheThreshold: the number of messages to be cached in memory. For performance reasons, the earliest cached messages are stored in memory so that they can be resent immediately upon failure recovery. Data here can be lost due to failures such as power outages.
- maxDiskCache: The maximum number of messages to be cached on disk. The disk cache is first-in, first-out. If the disk cache is full, the earliest page of information will be loaded into the memory cache, replacing the old memory cache.
- bufferPageSize. buffer pages are units of bulk reads/writes to disk to prevent frequent IO. if the pages are not full and eKuiper crashes due to hardware or software errors, the last unwritten pages to disk will be lost.
- resendInterval: The time interval to resend information after failure recovery to prevent message storms.
- cleanCacheAtStop: whether to clean all caches when the rule is stopped, to prevent mass resending of expired messages when the rule is restarted. If not set to true, the in-memory cache will be stored to disk once the rule is stopped. Otherwise, the memory and disk rules will be cleared out.

In the following example configuration of the rule, log sink has no cache-related options configured, so the global default configuration will be used; whereas mqtt sink performs its own caching policy configuration.

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM demo",
  "actions": [{
    "log": {},
    "mqtt": {
      "server": "tcp://127.0.0.1:1883",
      "topic": "result/cache",
      "qos": 0,
      "enableCache": true,
      "memoryCacheThreshold": 2048,
      "maxDiskCache": 204800,
      "bufferPageSize": 512,
      "resendInterval": 10
    }
  }]
}
```

## Resource Reuse

Like sources, actions also support configuration reuse. Users only need to create a yaml file with the same name as the target action in the sinks folder and write the configuration in the same form as the source.

For example, for the MQTT action scenario, the user can create the mqtt.yaml file in the sinks directory and write the following content

```yaml
test:
  qos: 1
  server: "tcp://broker.emqx.io:1883"
```

When users need MQTT actions, in addition to the traditional configuration method, as shown below

```json
    {
      "mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "devices/demo_001/messages/events/",
        "protocolVersion": "3.1.1",
        "qos": 1,
        "clientId": "demo_001",
        "username": "xyz.azure-devices.net/demo_001/?api-version=2018-06-30",
        "password": "SharedAccessSignature sr=*******************",
        "retained": false
      }
    }
```

Can also use the `resourceId` reference form with the following configuration

```json
{
      "mqtt": {
        "resourceId": "test",
        "topic": "devices/demo_001/messages/events/",
        "protocolVersion": "3.1.1",
        "clientId": "demo_001",
        "username": "xyz.azure-devices.net/demo_001/?api-version=2018-06-30",
        "password": "SharedAccessSignature sr=*******************",
        "retained": false
      }
}
```