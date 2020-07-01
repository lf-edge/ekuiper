# Basic configurations
The configuration file for Kuiper is at ``$kuiper/etc/kuiper.yaml``. The configuration file is yaml format.

## Log level

```yaml
basic:
  # true|false, with debug level, it prints more debug info
  debug: false
  # true|false, if it's set to true, then the log will be print to console
  consoleLog: false
  # true|false, if it's set to true, then the log will be print to log file
  fileLog: true
```

## Cli Port
```yaml
basic:
  # CLI port
  port: 20498
```
The port that the CLI server listens on

## Rest Service Configuration

```yaml
basic:
  # REST service port
  restPort: 9081
  restTls:
    certfile: /var/https-server.crt
    keyfile: /var/https-server.key
```

#### restPort
The port for the rest api http server to listen to.

#### restTls
The tls cert file path and key file path setting. If restTls is not set, the rest api server will listen on http. Otherwise, it will listen on https.

## Prometheus Configuration

Kuiper can export metrics to prometheus if ``prometheus`` option is true. The prometheus will be served with the port specified by ``prometheusPort`` option.

```yaml
basic:
  prometheus: true
  prometheusPort: 20499
```
For such a default configuration, Kuiper will export metrics and serve prometheus at ``http://localhost:20499/metrics``

