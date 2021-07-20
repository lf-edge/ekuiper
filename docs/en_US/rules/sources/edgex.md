## EdgeX Source

eKuiper provides built-in support for EdgeX source stream, which can subscribe the message from [EdgeX message bus](https://github.com/edgexfoundry/go-mod-messaging) and feed into the eKuiper streaming process pipeline.  

### Stream definition for EdgeX

EdgeX already defines data types in [readings](https://docs.edgexfoundry.org/2.0/microservices/core/data/Ch-CoreData/#events-and-readings), so it's recommended to use schema-less stream definition in EdgeX source as in below.

```shell
# cd $eKuiper_base
# bin/kuiper CREATE STREAM demo'() with(format="json", datasource="demo" type="edgex")'
```

EdgeX source will try to get the data type of fields, 

- convert to related data type if field of a type can be found in the readings's ValueType field;
- or keep original value if  field of a type can not be found in the readings's ValueType field;
- or if failed to conver the value, then the value will be **dropped**, and a warning message print in the log;

The types defined in readings will be converted into related [data types](../../sqls/streams.md) that supported in eKuiper.

#### Boolean

If ``ValueType`` value of the reading is ``Bool``, then eKuiper tries to convert to ``boolean`` type. Following values will be converted into ``true``.

- "1", "t", "T", "true", "TRUE", "True" 

Following will be converted into ``false``.

- "0", "f", "F", "false", "FALSE", "False"

#### Bigint

If ``ValueType`` value of the reading is ``INT8`` , ``INT16``, ``INT32``,  ``INT64``,``UINT`` , ``UINT8`` , ``UINT16`` ,  ``UINT32`` , ``UINT64`` then eKuiper tries to convert to ``Bigint`` type. 

#### Float

If ``ValueType`` value of the reading is ``FLOAT32``, ``FLOAT64``, then eKuiper tries to convert to ``Float`` type. 

#### String

If ``ValueType`` value of the reading is ``String``, then eKuiper tries to convert to ``String`` type. 

#### Boolean array

`Bool` array type in EdgeX will be converted to `boolean` array.

#### Bigint array

All of ``INT8`` , ``INT16``, ``INT32``,  ``INT64``,``UINT`` , ``UINT8`` , ``UINT16`` ,  ``UINT32`` , ``UINT64``  array types in EdgeX will be converted to `Bigint` array.

#### Float array

All of ``FLOAT32``, ``FLOAT64``  array types in EdgeX will be converted to `Float` array.

## Global configurations

The configuration file of EdgeX source is at ``$ekuiper/etc/sources/edgex.yaml``. Below is the file format.

```yaml
#Global Edgex configurations
default:
  protocol: tcp
  server: localhost
  port: 5573
  topic: events
  messageType: event
#  optional:
#    ClientId: client1
#    Username: user1
#    Password: password
```



Use can specify the global EdgeX settings here. The configuration items specified in ``default`` section will be taken as default settings for all EdgeX source. 

### protocol

The protocol connect to EdgeX message bus, default value is ``tcp``.

### server

The server address of  EdgeX message bus, default value is ``localhost``.

### port

The port of EdgeX message bus, default value is ``5573``.

### topic

The topic name of EdgeX message bus, default value is ``events``. Users can subscribe to the topics of message bus
directly or subscribe to topics exported by EdgeX application service. Notice that, the message type of the two types of
topics are different, remember to set the appropriate messageType property.

### type

The EdgeX message bus type, currently three types of message buses are supported. If specified other values, then will
use the default ``redis`` value.

- ``zero``: Use ZeroMQ as EdgeX message bus.
- ``mqtt``: Use the MQTT broker as EdgeX message bus.
- ``redis``: Use Redis as EdgeX message bus. When using EdgeX docker compose, the type will be set to this by default.

### messageType

The EdgeX message model type. If connected to the topic of EdgeX application service, the message model is an "event".
Otherwise, if connected to the topic of EdgeX message bus directly to receive the message from device service or core
data, the message is a "request". There are two available types of messageType property:

- ``event``: The message will be decoded as a `dtos.Event` type. This is the default.
- ``request``: The message will be decoded as a `requests.AddEventRequest` type.

### optional

If MQTT message bus is used, some other optional configurations can be specified. Please notice that all of values in
optional are **<u>string type</u>**, so values for these configurations should be string - such as ``KeepAlive: "5000"``
. Below optional configurations are supported, please check MQTT specification for the detailed information.

- ClientId

- Username
- Password
- Qos
- KeepAlive
- Retained
- ConnectionPayload
- CertFile
- KeyFile
- CertPEMBlock
- KeyPEMBlock
- SkipCertVerify

### Override the default settings

In some cases, maybe you want to consume message from multiple topics from message bus.  eKuiper supports to specify another configuration, and use the ``CONF_KEY`` to specify the newly created key when you create a stream.

```yaml
#Override the global configurations
demo1: #Conf_key
  protocol: tcp
  server: 10.211.55.6
  port: 5571
  topic: events
```

If you have a specific connection that need to overwrite the default settings, you can create a customized section. In the previous sample, we create a specific setting named with ``demo1``.  Then you can specify the configuration with option ``CONF_KEY`` when creating the stream definition (see [stream specs](../../sqls/streams.md) for more info).

**Sample**

```
create stream demo1() WITH (FORMAT="JSON", type="edgex", CONF_KEY="demo1");
```

The configuration keys used for these specific settings are the same as in ``default`` settings, any values specified in specific settings will overwrite the values in ``default`` section.

