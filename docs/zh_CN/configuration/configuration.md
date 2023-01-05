# 配置

eKuiper的配置是基于yaml文件，允许通过更新文件、环境变量和REST API进行配置。

## 配置范围

eKuiper的配置包括
1. `etc/kuiper.yaml`：全局配置文件。对其进行修改需要重新启动eKuiper实例。请参考[基本配置文件](./global_configurations.md)了解详情。
2. `etc/sources/${source_name}.yaml`：每个源的配置文件，用于定义默认属性（MQTT源除外，其配置文件为`etc/mqtt_source.yaml`）。详情请参考每个源的文档。例如，[MQTT 源](../guide/sources/builtin/mqtt.md)和[Neuron 源](../guide/sources/builtin/neuron.md)涵盖的配置项目。
3. `etc/connections/connection.yaml`：共享连接配置文件。

## 配置方法

用户可以通过3种方法设置配置，按优先级排序。

1. 管理控制台/REST API
2. 环境变量
3. etc文件夹中的Yaml文件

yaml 文件通常被用来设置默认配置。在裸机上部署时，用户可以很容易地访问文件系统，因此通常通过配置修改配置文件来更改配置。

当在docker或k8s中部署时，操作文件就不容易了，少量的配置可以通过环境变量来设置或覆盖。而在运行时，终端用户将使用管理控制台来动态地改变配置。eKuiper 管理控制台中的"配置"页面可以帮助用户直观地修改配置。

### 环境变量的语法

从环境变量到配置 yaml 文件之间有一个映射。当通过环境变量修改配置时，环境变量需要按照规定的格式来设置，例如。

```
KUIPER__BASIC__DEBUG => basic.debug in etc/kuiper.yaml
MQTT_SOURCE__DEMO_CONF__QOS => demo_conf.qos in etc/mqtt_source.yaml
EDGEX__DEFAULT__PORT => default.port in etc/sources/edgex.yaml
CONNECTION__EDGEX__REDISMSGBUS__PORT => edgex.redismsgbus.port int etc/connections/connection.yaml
```

环境变量用`__`分隔，分隔后的第一部分内容与配置文件的文件名匹配，其余内容与不同级别的配置项匹配。文件名可以是 `etc` 文件夹中的 `KUIPER` 和 `MQTT_SOURCE` ；或 `etc/connection` 文件夹中的`CONNECTION`。其余情况，映射文件应在 `etc/sources` 文件夹下。