# 基本配置
Kuiper 的配置文件位于 `$ kuiper / etc / kuiper.yaml` 中。 配置文件为 yaml 格式。

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
```
## 系统日志
用户将名为 KuiperSyslogKey 的环境变量的值设置为 true 时，日志将打印到系统日志中。
## Cli 端口
```yaml
basic:
  # CLI port
  port: 20498
```
CLI 服务器监听端口

## REST 服务配置

```yaml
basic:
  # REST service port
  restPort: 9081
  restTls:
    certfile: /var/https-server.crt
    keyfile: /var/https-server.key
```

#### restPort
REST http 服务器监听端口

#### restTls
TLS 证书 cert 文件和 key 文件位置。如果 restTls 选项未配置，则 REST 服务器将启动为 http 服务器，否则启动为 https 服务器。

## Prometheus 配置

如果 `prometheus` 参数设置为 true，Kuiper 将把运行指标暴露到 prometheus。Prometheus 将运行在 `prometheusPort` 参数指定的端口上。

```yaml
basic:
  prometheus: true
  prometheusPort: 20499
```
在如上默认配置中，Kuiper 暴露于 Prometheusd 运行指标可通过 `http://localhost:20499/metrics` 访问。

## Pluginhosts 配置

该 URL 对所有预构建插件托管。 默认情况下，它位于 `packages.emqx.io` 中。 可能有多个主机（主机可以用逗号分隔），如果可以在多个主机中找到相同的程序包，则第一个主机中的程序包将具有最高优先级。

请注意，只有可以安装到当前 Kuiper 实例的插件才会通过 Rest-API 下方列出。

```
GET http://localhost:9081/plugins/sources/prebuild
GET http://localhost:9081/plugins/sinks/prebuild
GET http://localhost:9081/plugins/functions/prebuild
```

如果希望通过前述的API列出插件，则应具有以下条件：

- Kuiper 版本：必须为 Kuiper 实例版本构建插件。 如果找不到特定版本的插件，则不会返回任何插件。
- 操作系统：现在仅支持 Linux 系统，因此，如果 Kuiper 在其他操作系统上运行，则不会返回任何插件。
- CPU 架构：只有在正确的 CPU 架构中构建的插件，才能在插件存储库中找到并返回。
- EMQ 官方发布的 Docker 映像：仅当 Kuiper 在 EMQ 官方发布的 Docker 映像上运行时，才能返回插件。

```yaml
pluginHosts: https://packages.emqx.io
```

具体如下所示，您可以指定本地存储库，该存储库中的插件将具有更高的优先级。

```yaml
pluginHosts: https://local.repo.net, https://packages.emqx.io
```

插件的目录结构应如下所示。

```
http://host:port/kuiper-plugins/0.9.1/alpine/sinks
http://host:port/kuiper-plugins/0.9.1/alpine/sources
http://host:port/kuiper-plugins/0.9.1/alpine/functions
```

页面内容如下所示。

```html
<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 3.2 Final//EN">
<html>
<title>Directory listing for enterprise: /4.1.1/</title>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8">
<meta name="robots" content="noindex,nofollow">
<body>
	<h2>Directory listing for enterprise: /4.1.1/</h2>
	<hr>
	<ul>
		<li><a href="file_386.zip">file_386.zip</a>
		<li><a href="file_amd64.zip">file_amd64.zip</a>
		<li><a href="file_arm.zip">file_arm.zip</a>
		<li><a href="file_arm64.zip">file_arm64.zip</a>
		<li><a href="file_ppc64le.zip">file_ppc64le.zip</a>

		<li><a href="influx_386.zip">influx_386.zip</a>
		<li><a href="influx_amd64.zip">influx_amd64.zip</a>
		<li><a href="influx_arm.zip">influx_arm.zip</a>
		<li><a href="influx_arm64.zip">influx_arm64.zip</a>
		<li><a href="influx_ppc64le.zip">influx_ppc64le.zip</a>
	</ul>
	<hr>
</body>
</html>
```



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

