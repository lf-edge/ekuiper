# MQTT source

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper provides built-in support for MQTT source stream, which can subscribe the message from MQTT broker and feed into the eKuiper processing pipeline.  The configuration file of MQTT source is at `$ekuiper/etc/mqtt_source.yaml`. Below is the file format.

```yaml
#Global MQTT configurations
default:
  qos: 1
  server: "tcp://127.0.0.1:1883"
  #username: user1
  #password: password
  #certificationPath: /var/kuiper/xyz-certificate.pem
  #privateKeyPath: /var/kuiper/xyz-private.pem.key
  #rootCaPath: /var/kuiper/xyz-rootca.pem
  #insecureSkipVerify: true
  #connectionSelector: mqtt.mqtt_conf1
  # Decompress the payload with the specified compression method. Support `gzip`, `zstd` method now.                                                                                                                                                                                                                                          
  # decompression: ""


#Override the global configurations
demo_conf: #Conf_key
  qos: 0
  server: "tcp://10.211.55.6:1883"

```

## Global MQTT configurations

Use can specify the global MQTT settings here. The configuration items specified in `default` section will be taken as default settings for all MQTT connections.

### qos

The default subscription QoS level.

### server

The server for MQTT message broker.

### username

The username for MQTT connection.

### password

The password for MQTT connection.

### decompression

Decompress the payload with the specified compression method. Support `gzip`, `zstd` method now.

### protocolVersion

MQTT protocol version. 3.1 (also refer as MQTT 3) or 3.1.1 (also refer as MQTT 4). If not specified, the default value is 3.1.

### clientid

The client id for MQTT connection. If not specified, an uuid will be used.

### certificationPath

The location of certification path. It can be an absolute path, or a relative path. If it is an relative path, then the base path is where you excuting the `kuiperd` command. For example, if you run `bin/kuiperd` from `/var/kuiper`, then the base path is `/var/kuiper`; If you run `./kuiperd` from `/var/kuiper/bin`, then the base path is `/var/kuiper/bin`.  Such as  `d3807d9fa5-certificate.pem`.

### privateKeyPath

The location of private key path. It can be an absolute path, or a relative path.  For more detailed information, please refer to `certificationPath`. Such as `d3807d9fa5-private.pem.key`.

### rootCaPath

The location of root ca path. It can be an absolute path, or a relative path.

### insecureSkipVerify

Control if to skip the certification verification. If it is set to true, then skip certification verification; Otherwise, verify the certification

### connectionSelector

specify the stream to reuse the connection to mqtt broker. The connection profile located in `connections/connection.yaml`.

```yaml
mqtt:
  localConnection: #connection key
    server: "tcp://127.0.0.1:1883"
    username: ekuiper
    password: password
    #certificationPath: /var/kuiper/xyz-certificate.pem
    #privateKeyPath: /var/kuiper/xyz-private.pem.ke
    #insecureSkipVerify: false
    #protocolVersion: 3
    clientid: ekuiper
  cloudConnection: #connection key
    server: "tcp://broker.emqx.io:1883"
    username: user1
    password: password
    #certificationPath: /var/kuiper/xyz-certificate.pem
    #privateKeyPath: /var/kuiper/xyz-private.pem.ke
    #insecureSkipVerify: false
    #protocolVersion: 3

```

There are two configuration groups for mqtt in the example, user need use `mqtt.localConnection` or `mqtt.cloudConnection` as the selector.
For example

```yaml
#Global MQTT configurations
default:
  qos: 1
  server: "tcp://127.0.0.1:1883"
  #username: user1
  #password: password
  #certificationPath: /var/kuiper/xyz-certificate.pem
  #privateKeyPath: /var/kuiper/xyz-private.pem.key
  connectionSelector: mqtt.localConnection
```

*Note*: once specify the connectionSelector in specific configuration group , all connection related parameters will be ignored , in this case ``servers: [tcp://127.0.0.1:1883]``

### bufferLength

specify the maximum number of messages to be buffered in the memory. This is used to avoid the extra large memory usage that would cause out of memory error. Notice that the memory usage will be varied to the actual buffer. Increase the length here won't increase the initial memory allocation so it is safe to set a large buffer length. The default value is 102400, that is if each payload size is about 100 bytes, the maximum buffer size will be about 102400 * 100B ~= 10MB.

### kubeedgeVersion

kubeedge version number. Different version numbers correspond to different file contents.

### kubeedgeModelFile

The name of the kubeedge template file. The file is located in the specified etc/sources folder. The sample format is as follows:

```json
{
  "deviceModels": [{
    "name": "device1",
    "properties": [{
      "name": "temperature",
      "dataType": "int"
    }, {
      "name": "temperature-enable",
      "dataType": "string"
    }]
  }]
}
```

#### deviceModels.name

The device name. It matches the field in the subscription topic that is located between the third and fourth "/". For example: $ke/events/device/device1/data/update.

#### properties.name

Field name.

#### properties.dataType

Expected field type.

## Override the default settings

If you have a specific connection that need to overwrite the default settings, you can create a customized section. In the previous sample, we create a specific setting named with `demo_conf`.  Then you can specify the configuration with option `CONF_KEY` when creating the stream definition (see [stream specs](../../../sqls/streams.md) for more info).

**Sample**

```text
demo (
    ...
  ) WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo_conf");
```

The configuration keys used for these specific settings are the same as in `default` settings, any values specified in specific settings will overwrite the values in `default` section.

## Use same connection selector in multiple customized config section

If user creates two config sections, for example

```yaml
#Override the global configurations
demo_conf: #Conf_key
  qos: 0
  connectionSelector: mqtt.localConnection 
  servers: [tcp://10.211.55.6:1883, tcp://127.0.0.1]

#Override the global configurations
demo2_conf: #Conf_key
  qos: 0
  connentionSelector: mqtt.localConnection
  servers: [tcp://10.211.55.6:1883, tcp://127.0.0.1]
```

create two streams using the config defined above

```text
demo (
    ...
  ) WITH (DATASOURCE="test/", FORMAT="JSON", CONF_KEY="demo_conf");

demo2 (
    ...
  ) WITH (DATASOURCE="test2/", FORMAT="JSON", CONF_KEY="demo2_conf");

```

When create rules using the defined streams, the rules will share the same connection in source part.
The `DATASOURCE` here will be used as mqtt subscription topics, and subscription  `Qos` defined in config section.
So stream `demo` will subscribe to topic `test/` with Qos 0 and stream `demo2` will subscribe to topic `test2/` with Qos 0 in this example.
But if  `DATASOURCE` is same and `qos` not, will only subscribe one time when the first rule starts.

## Migration Guide

Since 1.5.0, eKuiper changes the mqtt source broker configuration from `servers` to `server` and users can only configure a mqtt broker address instead of address array.
Users who are using mqtt broker as stream source in previous release and want to migrate to 1.5.0 release or later, need make sure the ``etc/mqtt_source.yaml`` file ``server`` 's configuration is right.
Users who are using environment variable to configure the mqtt source address need change their ENV successfully, for example, their broker address is ``tcp://broker.emqx.io:1883``. They need change the ENV from
``MQTT_SOURCE__DEFAULT__SERVERS=[tcp://broker.emqx.io:1883]`` to ``MQTT_SOURCE__DEFAULT__SERVER="tcp://broker.emqx.io:1883"``
