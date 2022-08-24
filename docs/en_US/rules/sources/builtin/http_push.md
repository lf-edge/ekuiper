# HTTP push source 

eKuiper provides built-in support for push HTTP source stream, which can receive the message from HTTP client.  The configuration file of HTTP push source is at `etc/sources/httppush.yaml`. Below is the file format.

```yaml
#Global httppull configurations
default:
  # the address to listen on
  server: ":8900"
    
#Override the global configurations
application_conf: #Conf_key
  server: ":9000"
```

## Global HTTP push configurations

Use can specify the global HTTP push settings here. The configuration items specified in `default` section will be taken as default settings for all HTTP connections. 

### server

The server address for http push listen on.


## Override the default settings

If you have a specific connection that need to overwrite the default settings, you can create a customized section. In the previous sample, we create a specific setting named with `application_conf`.  Then you can specify the configuration with option `CONF_KEY` when creating the stream definition (see [stream specs](../../../sqls/streams.md) for more info).

**Sample**

```
demo (
		...
	) WITH (DATASOURCE="/feed", FORMAT="JSON", TYPE="httppush", KEY="USERID", CONF_KEY="application_conf");
```

The configuration keys used for these specific settings are the same as in `default` settings, any values specified in specific settings will overwrite the values in `default` section.
