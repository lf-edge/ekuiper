# HTTP pull source 

eKuiper provides built-in support for pulling HTTP source stream, which can pull the message from HTTP server broker and feed into the eKuiper processing pipeline.  The configuration file of HTTP pull source is at `etc/sources/httppull.yaml`. Below is the file format.

```yaml
#Global httppull configurations
default:
  # url of the request server address
  url: http://localhost
  # post, get, put, delete
  method: post
  # The interval between the requests, time unit is ms
  interval: 10000
  # The timeout for http request, time unit is ms
  timeout: 5000
  # If it's set to true, then will compare with last result; If response of two requests are the same, then will skip sending out the result.
  # The possible setting could be: true/false
  incremental: false
  # The body of request, such as '{"data": "data", "method": 1}'
  body: '{}'
  # Body type, none|text|json|html|xml|javascript|form
  bodyType: json
  # HTTP headers required for the request
  insecureSkipVerify: true
  headers:
    Accept: application/json

#Override the global configurations
application_conf: #Conf_key
  incremental: true
  url: http://localhost:9090/pull
```

## Global HTTP pull configurations

Use can specify the global HTTP pull settings here. The configuration items specified in `default` section will be taken as default settings for all HTTP connections. 

### url

The URL where to get the result.

### method
HTTP method, it could be post, get, put & delete.

### interval

The interval between the requests, time unit is ms.

### timeout

The timeout for http request, time unit is ms.

### incremental

If it's set to true, then will compare with last result; If response of two requests are the same, then will skip sending out the result.

### body

The body of request, such as `'{"data": "data", "method": 1}'`

### bodyType

Body type, it could be none|text|json|html|xml|javascript|format.

### certificationPath

The location of certification path. It can be an absolute path, or a relative path. If it is an relative path, then the base path is where you excuting the `kuiperd` command. For example, if you run `bin/kuiperd` from `/var/kuiper`, then the base path is `/var/kuiper`; If you run `./kuiperd` from `/var/kuiper/bin`, then the base path is `/var/kuiper/bin`.  Such as  `d3807d9fa5-certificate.pem`.

### privateKeyPath

The location of private key path. It can be an absolute path, or a relative path.  For more detailed information, please refer to `certificationPath`. Such as `d3807d9fa5-private.pem.key`.

### rootCaPath

The location of root ca path. It can be an absolute path, or a relative path.

### insecureSkipVerify

Control if to skip the certification verification. If it is set to true, then skip certification verification; Otherwise, verify the certification

### headers

The HTTP request headers that you want to send along with the HTTP request.



## Override the default settings

If you have a specific connection that need to overwrite the default settings, you can create a customized section. In the previous sample, we create a specific setting named with `application_conf`.  Then you can specify the configuration with option `CONF_KEY` when creating the stream definition (see [stream specs](../../sqls/streams.md) for more info).

**Sample**

```
demo (
		...
	) WITH (DATASOURCE="test/", FORMAT="JSON", TYPE="httppull", KEY="USERID", CONF_KEY="application_conf");
```

The configuration keys used for these specific settings are the same as in `default` settings, any values specified in specific settings will overwrite the values in `default` section.

