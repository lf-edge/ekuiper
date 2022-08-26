# HTTP push 源

eKuiper 提供了内置的 HTTP 源，它作为一个 HTTP 服务器，可以接收来自 HTTP 客户端的消息。所有的 HTTP 推送源共用单一的全局 HTTP 数据服务器。每个源可以有自己的 URL，这样就可以支持多个端点。

## 配置

配置分成两个部分：全局服务器配置和源配置。

### 服务器配置

服务器配置在 `etc/kuiper.yaml` 中的 `source` 部分。

```yaml
source:
  ## Configurations for the global http data server for httppush source
  # HTTP data service ip
  httpServerIp: 0.0.0.0
  # HTTP data service port
  httpServerPort: 10081
  # httpServerTls:
  #    certfile: /var/https-server.crt
  #    keyfile: /var/https-server.key
```

用户可以指定以下属性：

- httpServerIp：用于绑定 http 数据服务器的IP。
- httpServerPort：用于绑定 http 数据服务器的端口。
- httpServerTls: http 服务器 TLS 的配置。

一旦有任何需要 httppush 源的规则启动，全局服务器就会启动。一旦所有引用的规则都关闭，它就会关闭。

### 源配置

每个流可以配置它的 URL 端点和 http 请求方法。端点属性被映射到创建流语句中的 `datasource` 属性。

在以下示例中，源被绑定到 `/api/data` 端点。此时，在默认的服务器配置下，它将监听`http://localhost:10081/api/data` 。

```sql
CREATE STREAM httpDemo() WITH (DATASOURCE="/api/data", FORMAT="json", TYPE="httppush")
```

HTTP 推送源的配置文件在 `etc/sources/httppush.yaml` 。目前仅一个属性 `method` ，用于配置 HTTP 监听的请求方法。

```yaml
#Global httppush configurations
default:
  # the request method to listen on
  method: "POST"
    
#Override the global configurations
application_conf: #Conf_key
  server: "PUT"
```