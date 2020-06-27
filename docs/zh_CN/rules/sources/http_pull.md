# HTTP 提取源

Kuiper为提取HTTP源流提供了内置支持，该支持可从HTTP服务器代理提取消息并输入Kuiper处理管道。 HTTP提取源的配置文件位于 ``etc/sources/httppull.yaml``中。 以下是文件格式。

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
  headers:
    Accept: application/json

#Override the global configurations
application_conf: #Conf_key
  incremental: true
  url: http://localhost:9090/pull
```

## 全局HTTP提取配置

用户可以在此处指定全局HTTP提取设置。 ``default``部分中指定的配置项将用作所有HTTP连接的默认设置。

### url

获取结果的URL。

### method
HTTP方法，它可以是post、get、put和delete。

### interval

请求之间的间隔时间，单位为ms。

### timeout

http请求的超时时间，单位为ms。

### incremental

如果将其设置为true，则将与最后的结果进行比较； 如果两个请求的响应相同，则将跳过发送结果。

### body

请求的结构体, 例如`'{"data": "data", "method": 1}'`

### bodyType

结构体类型,可以是none|text|json|html|xml|javascript|格式。

### headers

需要与HTTP请求一起发送的HTTP请求标头。



## 覆盖默认设置

如果您有特定的连接需要覆盖默认设置，则可以创建一个自定义部分。 在上一个示例中，我们创建了一个名为``application_conf``的特定设置。 然后，您可以在创建流定义时使用选项``CONF_KEY``指定配置（有关更多信息，请参见 [流规格](../../sqls/streams.md)）。

**样例**

```
demo (
		...
	) WITH (DATASOURCE="test/", FORMAT="JSON", TYPE="httppull", KEY="USERID", CONF_KEY="application_conf");
```

这些特定设置所使用的配置键与 ``default`` 设置中的配置键相同，在特定设置中指定的任何值都将覆盖 ``default`` 部分中的值。

