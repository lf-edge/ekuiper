# Basic configurations
The configuration file for Kuiper is at ``$kuiper/etc/kuiper.yaml``. The configuration file is yaml format.

## Log level

```yaml
basic:
  # true|false, with debug level, it prints more debug info
  debug: false
```

## Prometheus Configuration

Kuiper can export metrics to prometheus if ``prometheus`` option is true. The prometheus will be served with the port specified by ``prometheusPort`` option.

```yaml
basic:
  prometheus: true
  prometheusPort: 20499
```
For such a default configuration, Kuiper will export metrics and serve prometheus at ``http://localhost:20499/metrics``

