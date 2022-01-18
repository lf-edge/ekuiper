# 基本配置
eKuiper 的配置文件位于 `$ eKuiper / etc / kuiper.yaml` 中。 配置文件为 yaml 格式。

## 日志级别

```yaml
basic:
  # true|false, with debug level, it prints more debug info
  debug: false
  # true|false, if it's set to true, then the log will be print to console
  consoleLog: false
  # true|false, if it's set to true, then the log will be print to log file
  fileLog: true
  # How many hours to split the file
  rotateTime: 24
  # Maximum file storage hours
  maxAge: 168
  # Whether to ignore case in SQL processing. Note that, the name of customized function by plugins are case-sensitive.
  ignoreCase: true
```

配置项 **ignoreCase** 用于指定 SQL 处理中是否大小写无关。默认情况下，为了与标准 SQL 兼容，其值为 true 。从而使得输入数据的列名大小写可以与 SQL 中的定义不同。如果 SQL 语句中，流定义以及输入数据中可以保证列名大小写完全一致，则建议设置该值为 false 以获得更优的性能。

## 系统日志
用户将名为 KuiperSyslogKey 的环境变量的值设置为 true 时，日志将打印到系统日志中。
## Cli 地址
```yaml
basic:
  # CLI 绑定 IP
  ip: 0.0.0.0
  # CLI port
  port: 20498
```
## REST 服务配置

```yaml
basic:
  # REST service 绑定 IP
  restIp: 0.0.0.0
  # REST service port
  restPort: 9081
  restTls:
    certfile: /var/https-server.crt
    keyfile: /var/https-server.key
```

### restPort
REST http 服务器监听端口

### restTls
TLS 证书 cert 文件和 key 文件位置。如果 restTls 选项未配置，则 REST 服务器将启动为 http 服务器，否则启动为 https 服务器。

## authentication 
当 `authentication` 选项为 true 时，eKuiper 将为 rest api 请求检查 `Token` 。请检查此文件以获取 [更多信息](authentication.md)。

```yaml
basic:
  authentication: false
```


## Prometheus 配置

如果 `prometheus` 参数设置为 true，eKuiper 将把运行指标暴露到 prometheus。Prometheus 将运行在 `prometheusPort` 参数指定的端口上。

```yaml
basic:
  prometheus: true
  prometheusPort: 20499
```
在如上默认配置中，eKuiper 暴露于 Prometheusd 运行指标可通过 `http://localhost:20499/metrics` 访问。

## Pluginhosts 配置

默认在 `packages.emqx.net` 托管所有预构建 [native 插件](../../extension/native/overview.md)。

所有插件列表如下：

|  插件类型        |        预构建插件列表                                             |
|  -----------   |  ---------------------------------------------------------------|
|  source        |    random zmq                                                   |
|  sink          |  file image influx redis tdengine zmq                           |
|  function      |  accumulateWordCount countPlusOne echo geohash image labelImage |

用户可以通过以下 Rest-API 获取所有预构建插件的名称和地址：

```
GET http://localhost:9081/plugins/sources/prebuild
GET http://localhost:9081/plugins/sinks/prebuild
GET http://localhost:9081/plugins/functions/prebuild
``` 

获取插件信息后，用户可以尝试这些插件，[更多信息](../restapi/plugins.md)

**注意：只有官方发布的基于 debian 的 docker 镜像支持以上操作**

## Sink 配置

```yaml
  #缓存持久化阈值。 如果接收器高速缓存中的消息大于10，则它将触发持久化。 如果发现远程系统响应速度慢或接收器吞吐量很小，则建议增加2种以下配置，此时需要更多内存。

  # 如果消息计数达到以下值，则会触发持久化。
  cacheThreshold: 10
  # 消息持久化由代码触发，cacheTriggerCount 用于使用配置计数来触发持久化过程，而不管消息号是否达到cacheThreshold。 这是为了防止由于缓存永远不会超过阈值而无法保存数据。
  cacheTriggerCount: 15

  # 控制是否禁用缓存。 如果将其设置为true，则将禁用缓存，否则将启用缓存。
  disableCache: false
```

## 存储配置

可通过配置修改创建的流和规则等状态的存储方式。默认情况下，程序状态存储在 sqlite 数据库中。把存储类型改成 redis，可使用 redis 作为存储方式。

### Sqlite

可配置如下属性：
* name - 数据库文件名。若为空，则设置为默认名字`sqliteKV.db`。

### Redis

可配置如下属性：
* host     - redis 服务器地址。
* port     - redis 服务器端口。
* password - redis 服务器密码。若 redis 未配置认证系统，则可不设置密码。
* timeout  - 连接超时时间。
* connectionSelector - 重用 etc/connections/connection.yaml 中定义的连接信息, 主要用在 edgex redis 配置了认证系统时
    * 只适用于 edgex redis 的连接信息 
    * 连接信息中的 server， port 和 password 会覆盖以上定义的 host， port 和 password
    * [具体信息可参考](../../rules/sources/edgex.md#connectionselector)


### 配置示例

```yaml
    store:
      #Type of store that will be used for keeping state of the application
      type: sqlite
      redis:
        host: localhost
        port: 6379
        password: kuiper
        #Timeout in ms
        timeout: 1000
      sqlite:
        #Sqlite file name, if left empty name of db will be sqliteKV.db
        name:
```

## Portable 插件配置

配置 portable 插件的运行时属性。

```yaml
  portable:
      # 配置 python 可执行文件的位置或命令。
      # 若系统中有多个 python 版本，可通过此配置指定具体的 python 地址。
      pythonBin: python
```