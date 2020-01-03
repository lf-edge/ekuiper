# MQTT source 

Kuiper provides built-in support for MQTT source stream, which can subscribe the message from MQTT broker and feed into the Kuiper processing pipeline.  The configuration file of MQTT source is at ``$kuiper/etc/mqtt_source.yaml``. Below is the file format.

```yaml
#Global MQTT configurations
default:
  qos: 1
  sharedSubscription: true
  servers: [tcp://127.0.0.1:1883]
  concurrency: 1
  #username: user1
  #password: password
  #certificationPath: /var/kuiper/xyz-certificate.pem
  #privateKeyPath: /var/kuiper/xyz-private.pem.key


#Override the global configurations
demo_conf: #Conf_key
  qos: 0
  servers: [tcp://10.211.55.6:1883, tcp://127.0.0.1]

```

## Global MQTT configurations

Use can specify the global MQTT settings here. The configuration items specified in ``default`` section will be taken as default settings for all MQTT connections. 

### qos

The default subscription QoS level.

### concurrency
How many instances will be started. By default, only an instance will be run. If more than one instance is specified, the topic must be a shared subscription topic.

### sharedSubscription

Whether use the shared subscription mode or not. If using the shared subscription mode, then there are multiple Kuiper process can be load balanced.

### servers

The server list for MQTT message broker. Currently, only ``ONE`` server can be specified.

### username

The username for MQTT connection. The configuration will not be used if ``certificationPath`` or ``privateKeyPath`` is specified.

### password

The password for MQTT connection. The configuration will not be used if ``certificationPath`` or ``privateKeyPath`` is specified.

### certificationPath

The location of certification path. It can be an absolute path, or a relative path. If it is an relative path, then the base path is where you excuting the ``server`` command. For example, if you run ``bin/server`` from ``/var/kuiper``, then the base path is ``/var/kuiper``; If you run ``./server`` from ``/var/kuiper/bin``, then the base path is ``/var/kuiper/bin``.  Such as  ``d3807d9fa5-certificate.pem``.

### privateKeyPath

The location of private key path. It can be an absolute path, or a relative path.  For more detailed information, please refer to ``certificationPath``. Such as ``d3807d9fa5-private.pem.key``.

### bufferLength
specify the maximum number of messages to be buffered in the memory. This is used to avoid the extra large memory usage that would cause out of memory error. Notice that the memory usage will be varied to the actual buffer. Increase the length here won't increase the initial memory allocation so it is safe to set a large buffer length. The default value is 102400, that is if each payload size is about 100 bytes, the maximum buffer size will be about 102400 * 100B ~= 10MB.

## Override the default settings

If you have a specific connection that need to overwrite the default settings, you can create a customized section. In the previous sample, we create a specific setting named with ``demo``.  Then you can specify the configuration with option ``CONF_KEY`` when creating the stream definition (see [stream specs](../../sqls/streams.md) for more info).

**Sample**

```
demo (
		...
	) WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo");
```

The configuration keys used for these specific settings are the same as in ``default`` settings, any values specified in specific settings will overwrite the values in ``default`` section.

