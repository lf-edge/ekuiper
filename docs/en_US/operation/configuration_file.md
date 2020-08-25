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

## Sink configurations

```yaml
  #The cache persistence threshold size. If the message in sink cache is larger than 10, then it triggers persistence. If you find the remote system is slow to response, or sink throughput is small, then it's recommend to increase below 2 configurations.More memory is required with the increase of below 2 configurations.

  # If the message count reaches below value, then it triggers persistence.
  cacheThreshold: 10
  # The message persistence is triggered by a ticker, and cacheTriggerCount is for using configure the count to trigger the persistence procedure regardless if the message number reaches cacheThreshold or not. This is to prevent the data won't be saved as the cache never pass the threshold.
  cacheTriggerCount: 15

  # Control to disable cache or not. If it's set to true, then the cache will be disabled, otherwise, it will be enabled.
  disableCache: false
```

