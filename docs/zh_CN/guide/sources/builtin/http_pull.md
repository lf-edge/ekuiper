# HTTP Pull 数据源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper 内置支持 HTTP 数据源。通过 HTTP Pull 数据源连接器，eKuiper 可从外部 HTTP 服务器检索数据，并支持基于指定间隔或由特定条件触发拉取数据。

## 配置

eKuiper 连接器可以通过[环境变量](../../../configuration/configuration.md#environment-variable-syntax)、[REST API](../../../api/restapi/configKey.md) 或配置文件进行配置，本节将介绍配置文件的使用方法。

HTTP Pull 源连接器的配置文件位于：`$ekuiper/etc/sources/http_pull.yaml`，其中：

- default：对应全局连接配置。
- 自定义部分：对于需要自定义某些连接参数的场景，该部分的配置将覆盖全局连接配置。

以下示例包括一个全局配置和自定义配置 `application_conf`：

```yaml
#全局 httppull 配置
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
  # 正文类型, none、text、json、html、xml、javascript、form
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

## 全局配置

用户可在 `default` 部分指定全局设置。

### **HTTP 请求配置**

- `url`：获取结果的 URL。
- `method`：HTTP 方法，支持 post、get、put 和 delete。
- `interval`：请求之间的间隔时间，单位为 ms。
- `timeout`：HTTP 请求的超时时间，单位为 ms。
- `body`：请求的正文，例如 `{"data": "data", "method": 1}`
- `bodyType`：正文类型，可选值 none、text、json、html、xml、javascript、format.
- `headers`：需要与 HTTP 请求一起发送的 HTTP 请求标头。
- `responseType`：定义如何解析 HTTP 响应。目前支持两种方式：
  - `code`：通过 HTTP 响应码判断响应状态。
  - `body`：通过 HTTP 响应正文判断响应状态。要求响应正文为 JSON 格式且其中包含 code 字段。

### 安全配置

#### 证书路径

- `certificationPath`:  证书路径，示例值：`d3807d9fa5-certificate.pem`。可以是绝对路径，也可以是相对路径。如指定相对路径，那么父目录为执行 `kuiperd` 命令的路径，例如：
  - 如果在 `/var/kuiper` 中运行 `bin/kuiperd` ，那么父目录为 `/var/kuiper`。
  - 如果运行从`/var/kuiper/bin`中运行`./kuiperd`，那么父目录为 `/var/kuiper/bin`。
- `privateKeyPath`：私钥路径，示例值：`d3807d9fa5-private.pem.key`。可以是绝对路径，也可以是相对路径，具体可参考 `certificationPath`。
- `rootCaPath`：根证书路径。可以是绝对路径，也可以是相对路径。
- `insecureSkipVerify`：是否跳过证书验证。如设置为 `true`，TLS 接受服务器提供的任何证书以及该证书中的任何主机名。注意：此时，TLS 容易受到中间人攻击。默认值：`false`。

#### OAuth 认证

OAuth 2.0 是一个授权协议，让 API 客户端有限度地访问网络服务器上的用户数据。oAuth 最常见的流程是授权代码，大多用于服务器端和移动网络应用。在这个流程中，用户使用他们的账户登录到网络应用中，认证码会返回给应用。之后，应用程序可以使用认证码来请求访问令牌，并可能在到期后通过刷新令牌来刷新令牌。

以下配置假设已经获取认证码，用户只需指定令牌申请，该过程可能需要该认证码或只是密码（OAuth 的变体）。

`OAuth`：定义类 OAuth 的认证流程。其他的认证方式如 apikey 可以直接在 headers 设置密钥，不需要使用这个配置。

- `access`

  - `url`：获取访问码的网址，将始终使用 POST 方法访问。

  - `body`：获取访问令牌的请求主体。通常需要授权码。

  - `expire`：令牌的过期时间，单位：秒，允许使用模板，因此必须是字符串。

- `refresh`

  - `url`：刷新令牌的网址，将始终使用 POST 方法访问。

  - `headers`：用于刷新令牌的请求头。通常把令牌放在这里，用于授权。

  - `body`：刷新令牌的请求主体。当使用头文件来传递刷新令牌时，可能不需要配置此选项。

### 数据处理配置

#### 增量数据处理

`incremental`：如设置为 `true`，则将与上次的结果进行比较；如果两次请求的响应相同，则将跳过发送结果。

#### 动态属性

动态属性是指在运行时会动态更新的属性。您可以使用动态属性来指定 HTTP 请求的 URL、正文和标头。其语法基于[数据模板](../../sinks/data_template.md)格式的动态属性。

可使用的动态属性包括：

- `PullTime`：本次拉取的 int64 格式时间戳。
- `LastPullTime`：上次拉取的 int64 格式时间戳。

若目标 HTTP 服务支持过滤开始和结束时间，可以使用这两个属性来实现增量拉取。

::: v-pre

- 通过 URL 参数传递开始和结束时间：`http://localhost:9090/pull?start={{.LastPullTime}}&end={{.PullTime}}`.
- 通过 body 参数传递开始和结束时间：{"start": {{.LastPullTime}}, "end": {{.PullTime}}`.

:::

## 自定义配置

对于需要自定义某些连接参数的场景，eKuiper 支持用户创建自定义模块来实现全局配置的重载。

**配置示例**

```yaml
#覆盖全局配置
application_conf: #Conf_key
  incremental: true
  url: http://localhost:9090/pull
```

定义  `application_conf`  配置组后，如希望在创建流时使用此配置，可通过 `CONF_KEY` 选项并指定配置名称，此时，在自定义配置中定义的参数将覆盖 `default` 配置中的相应参数。详细步骤，可参考 [流语句](../../../sqls/streams.md)。

**示例**

```json
demo (
    ...
  ) WITH (DATASOURCE="test/", FORMAT="JSON", TYPE="httppull", KEY="USERID", CONF_KEY="application_conf");
```

## 创建流类型源

完成连接器的配置后，后续可通过创建流将其与 eKuiper 规则集成。HTTP Pull 数据源连接器可以作为[流式](../../streams/overview.md)或[扫描表类数据源](../../tables/scan.md)使用，本节将以流类型源为例进行说明。

您可通过 REST API 或 CLI 工具在 eKuiper 中创建 HTTP Pull 数据源。

### 通过 REST API 创建

REST API 为 eKuiper 提供了一种可编程的交互方式，适用于自动化或需要将 eKuiper 集成到其他系统中的场景。

**示例**

```sql
{"sql":"create stream http_stream () WITH (FORMAT="json", TYPE="http_pull"}
```

详细操作步骤及命令解释，可参考 [通过 REST API 进行流管理](../../../api/restapi/streams.md)。

### 通过 CLI 创建

用户也可以通过命令行界面（CLI）直接访问 eKuiper。

1. 进入 eKuiper `bin` 目录：

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. 使用 `create` 命令创建规则，指定 HTTP Pull 数据源，如：

   ```bash
   bin/kuiper create stream http_stream '() WITH (FORMAT="json", TYPE="http_pull")'
   ```

详细操作步骤及命令解释，可参考 [通过 CLI 进行流管理](../../../api/cli/streams.md)。

## 查询表

httppull 同时也支持成为一个查询表。我们可以使用创建表语句来创建一个 httppull 查询表。它将与实体关系数据库绑定并按需查询:

```text
CREATE TABLE httppullTable() WITH (DATASOURCE="/url", CONF_KEY="default", TYPE="httppull", KIND="lookup")
```
