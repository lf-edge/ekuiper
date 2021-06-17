# HTTP 提取源

eKuiper 为提取 HTTP 源流提供了内置支持，该支持可从 HTTP 服务器代理提取消息并输入 eKuiper 处理管道。 HTTP提取源的配置文件位于 `etc/sources/httppull.yaml`中。 以下是文件格式。

```yaml
#全局httppull配置
default:
  # 请求服务器地址的URL
  url: http://localhost
  # post, get, put, delete
  method: post
  # 请求之间的间隔，时间单位为 ms
  interval: 10000
  # http请求超时，时间单位为 ms
  timeout: 5000
  # 如果将其设置为 true，则将与最后的结果进行比较； 如果两个请求的响应相同，则将跳过发送结果。
  # 可能的设置可能是：true/false
  incremental: false
  # 请求正文，例如'{"data": "data", "method": 1}'
  body: '{}'
  # 正文类型, none|text|json|html|xml|javascript|form
  bodyType: json
  # 请求所需的HTTP标头
  headers:
    Accept: application/json

#重载全局配置
application_conf: #Conf_key
  incremental: true
  url: http://localhost:9090/pull
```

## 全局HTTP提取配置

用户可以在此处指定全局 HTTP 提取设置。 `default` 部分中指定的配置项将用作所有HTTP 连接的默认设置。

### url

获取结果的 URL。

### method
HTTP 方法，它可以是 post、get、put 和 delete。

### interval

请求之间的间隔时间，单位为 ms。

### timeout

http 请求的超时时间，单位为 ms。

### incremental

如果将其设置为 true，则将与最后的结果进行比较； 如果两个请求的响应相同，则将跳过发送结果。

### body

请求的正文, 例如 `{"data": "data", "method": 1}`

### bodyType

正文类型,可以是 none|text|json|html|xml|javascript|form。

### headers

需要与 HTTP 请求一起发送的 HTTP 请求标头。



## 重载默认设置

如果您有特定的连接需要重载默认设置，则可以创建一个自定义部分。 在上一个示例中，我们创建了一个名为 `application_conf` 的特定设置。 然后，您可以在创建流定义时使用选项 `CONF_KEY` 指定配置（有关更多信息，请参见 [流规格](../../sqls/streams.md)）。

**样例**

```
demo (
		...
	) WITH (DATASOURCE="test/", FORMAT="JSON", TYPE="httppull", KEY="USERID", CONF_KEY="application_conf");
```

这些特定设置所使用的配置键与 `default` 设置中的配置键相同，在特定设置中指定的任何值都将重载 `default` 部分中的值。

