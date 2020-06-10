# 基本配置
Kuiper的配置文件位于$ kuiper / etc / kuiper.yaml中。 配置文件为yaml格式。

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
## Cli端口
```yaml
basic:
  # CLI port
  port: 20498
```
CLI服务器监听端口

## REST服务配置

```yaml
basic:
  # REST service port
  restPort: 9081
  restTls:
    certfile: https-server.crt
    keyfile: https-server.key
```

#### restPort
REST http服务器监听端口

#### restTls
TLS证书cert文件和key文件位置。如果restTls选项未配置，则REST服务器将启动为http服务器，否则启动为https服务器。

## Prometheus配置

如果``prometheus``参数设置为true，Kuiper 将把运行指标暴露到prometheus。Prometheus将运行在``prometheusPort``参数指定的端口上。

```yaml
basic:
  prometheus: true
  prometheusPort: 20499
```
在如上默认配置中，Kuiper暴露于Prometheusd 运行指标可通过``http://localhost:20499/metrics``访问。

