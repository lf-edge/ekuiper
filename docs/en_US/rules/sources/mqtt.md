# MQTT source 

Kuiper provides built-in support for MQTT source stream, which can subscribe the message from MQTT broker and feed into the Kuiper processing pipeline.  The configuration file of MQTT source is at ``$kuiper/etc/mqtt_source.yaml``. Below is the file format.

```yaml
#Global MQTT configurations
default:
  qos: 1
  sharedsubscription: true
  servers: [tcp://127.0.0.1:1883]
  #TODO: Other global configurations


#Override the global configurations
demo: #Conf_key
  qos: 0
  servers: [tcp://10.211.55.6:1883]
```

## Global MQTT configurations

Use can specify the global MQTT settings here. The configuration items specified in ``default`` section will be taken as default settings for all MQTT connections. 

### qos

The default subscription QoS level.

### sharedsubscription

Whether use the shared subscription mode or not. If using the shared subscription mode, then if there are multiple Kuiper process can be load balanced.

### servers

The server list for MQTT message broker. Currently, only ``ONE`` server can be specified.

## Override the default settings

If you have a specific connection that need to overwrite the default settings, you can create a customized section. In the previous sample, we create a specific setting named with ``demo``.  Then you can specify the configuration with option ``CONF_KEY`` when creating the stream definition (see [stream specs](../../sqls/streams.md) for more info).

**Sample**

```
demo (
		...
	) WITH (datasource="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo");
```

The configuration keys used for these specific settings are the same as in ``default`` settings, any values specified in specific settings will overwrite the values in ``default`` section.

