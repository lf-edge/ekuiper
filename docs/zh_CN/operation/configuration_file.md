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
```
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

The URL where hosts all of pre-build plugins. By default it's at `packages.emqx.io`. There could be several hosts (host can be separated with comma), if same package could be found in the several hosts, then the package in the 1st host will have the highest priority.

Please notice that only the plugins that can be installed to the current Kuiper instance will be listed through below Rest-APIs.  

```
GET http://localhost:9081/plugins/sources/prebuild
GET http://localhost:9081/plugins/sinks/prebuild
GET http://localhost:9081/plugins/functions/prebuild
```

It has following conditions to make the plugins listed through previous APIs,

- Kuiper version: The plugins must be built for the Kuiper instance version. If the plugins cannot be found  for a specific version, no plugins will be returned.
- Operating system: Now only Linux system is supported, so if Kuiper is running at other operating systems,  no plugins will be returned.
- CPU architecture: Only with correct CPU architecture built plugins are found in the plugin repository can the plugins be returned.
- EMQ official released Docker images: Only when the Kuiper is running at EMQ official released Docker images can the plugins be returned.

```yaml
pluginHosts: https://packages.emqx.io
```

It could be also as following, you can specify a local repository, and the plugin in that repository will have higher priorities.

```yaml
pluginHosts: https://local.repo.net, https://packages.emqx.io
```

The directory structure of the plugins should be similar as following.

```
http://host:port/kuiper-plugins/0.9.1/sinks/alpine
```

The content of the page should be similar as below.

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
  #The cache persistence threshold size. If the message in sink cache is larger than 10, then it triggers persistence. If you find the remote system is slow to response, or sink throughput is small, then it's recommend to increase below 2 configurations.More memory is required with the increase of below 2 configurations.

  # If the message count reaches below value, then it triggers persistence.
  cacheThreshold: 10
  # The message persistence is triggered by a ticker, and cacheTriggerCount is for using configure the count to trigger the persistence procedure regardless if the message number reaches cacheThreshold or not. This is to prevent the data won't be saved as the cache never pass the threshold.
  cacheTriggerCount: 15

  # Control to disable cache or not. If it's set to true, then the cache will be disabled, otherwise, it will be enabled.
  disableCache: false
```

