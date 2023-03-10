# HTTP 提取源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

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
  # 如何检查响应状态，支持通过状态码或 body
  responseType: code
  # 获取 token
#  oAuth:
#    # 设置如何获取访问码
#    access:
#      # 获取访问码的 URL，总是使用 POST 方法发送请求
#      url: https://127.0.0.1/api/token
#      # 请求正文
#      body: '{"username": "admin","password": "123456"}'
#      # 令牌的过期时间，以字符串表示，时间单位为秒，允许使用模板
#      expire: '3600'
#    # 如何刷新令牌
#    refresh:
#      # 刷新令牌的 URL，总是使用 POST 方法发送请求
#      url: https://127.0.0.1/api/refresh
#      # HTTP 请求头，允许使用模板
#      headers:
#        identityId: '{{.data.identityId}}'
#        token: '{{.data.token}}'
#      # 请求正文
#      body: ''

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

### certificationPath

证书路径。可以为绝对路径，也可以为相对路径。如果指定的是相对路径，那么父目录为执行 `kuiperd` 命令的路径。比如，如果你在 `/var/kuiper` 中运行 `bin/kuiperd` ，那么父目录为 `/var/kuiper`; 如果运行从`/var/kuiper/bin`中运行`./kuiperd`，那么父目录为 `/var/kuiper/bin`。 比如  `d3807d9fa5-certificate.pem`。

### privateKeyPath

私钥路径。可以为绝对路径，也可以为相对路径。更详细的信息，请参考 `certificationPath`，比如 `d3807d9fa5-private.pem.key`。

### rootCaPath

根证书路径。可以为绝对路径，也可以为相对路径。

### insecureSkipVerify 
控制是否跳过证书认证。如果被设置为 `true`，那么跳过证书认证；否则进行证书验证。缺省为 `true`

### headers

需要与 HTTP 请求一起发送的 HTTP 请求标头。

### 响应类型

定义如何解析 HTTP 响应。目前支持两种方式：
- code：通过 HTTP 响应码判断响应状态。
- body：通过 HTTP 响应正文判断响应状态。要求响应正文为 JSON 格式且其中包含 code
 字段。

### OAuth

定义类 OAuth 的认证流程。其他的认证方式如 apikey 可以直接在 headers 设置密钥，不需要使用这个配置。

OAuth 2.0 是一个授权协议，让 API 客户端有限度地访问网络服务器上的用户数据。oAuth 最常见的流程是授权代码，大多用于服务器端和移动网络应用。在这个流程中，用户使用他们的账户登录到网络应用中，认证码会返回给应用。之后，应用程序可以使用认证码来请求访问令牌，并可能在到期后通过刷新令牌来刷新令牌。

在这个配置中，我们假设认证码已经获取了，用户只需指定令牌申请的过程，该过程可能需要该认证码或只是密码（OAuth 的变体）。

需要配置两个部分：用于获取访问代码的 access 配置和用于令牌刷新的 refresh 配置。其中，refresh 配置是可选的，只有存在单独的刷新流程时才需要配置。

#### access

- url：获取访问码的网址，总是使用POST方法访问。
- body：获取访问令牌的请求主体。通常情况下，可在这里提供授权码。
- expire：令牌的过期时间，时间单位是秒，允许使用模板，所以必须是一个字符串。

#### refresh

- url：刷新令牌的网址，总是使用POST方式请求。
- headers：用于刷新令牌的请求头。通常把令牌放在这里，用于授权。
- body：刷新令牌的请求主体。当使用头文件来传递刷新令牌时，可能不需要配置此选项。

## 重载默认设置

如果您有特定的连接需要重载默认设置，则可以创建一个自定义部分。 在上一个示例中，我们创建了一个名为 `application_conf` 的特定设置。 然后，您可以在创建流定义时使用选项 `CONF_KEY` 指定配置（有关更多信息，请参见 [流规格](../../../sqls/streams.md)）。

**样例**

```
demo (
		...
	) WITH (DATASOURCE="test/", FORMAT="JSON", TYPE="httppull", KEY="USERID", CONF_KEY="application_conf");
```

这些特定设置所使用的配置键与 `default` 设置中的配置键相同，在特定设置中指定的任何值都将重载 `default` 部分中的值。

