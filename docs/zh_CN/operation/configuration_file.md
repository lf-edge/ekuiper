# 基本配置
Kuiper的配置文件位于$ kuiper / etc / kuiper.yaml中。 配置文件为yaml格式。

## 日志级别

```yaml
basic:
  # true|false, with debug level, it prints more debug info
  debug: false
```

## Prometheus配置

如果``prometheus``参数设置为true，Kuiper 将把运行指标暴露到prometheus。Prometheus将运行在``prometheusPort``参数指定的端口上。

```yaml
basic:
  prometheus: true
  prometheusPort: 20499
```
在如上默认配置中，Kuiper暴露于Prometheusd 运行指标可通过``http://localhost:20499/metrics``访问。

