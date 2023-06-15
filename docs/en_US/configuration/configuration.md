# Configuration

eKuiper configuration is based on yaml file and allow to configure by updating the file, environment variable and REST API.

## Configuration Scope

eKuiper configurations include

1. `etc/kuiper.yaml`: global configuration file. Make change to it need to restart the eKuiper instance. Please refer to [basic configuration file](./global_configurations.md) for detail.
2. `etc/sources/${source_name}.yaml`: the configuration file for each source to define the default properties (except MQTT source, whose configuration file is `etc/mqtt_source.yaml`). Please refer to the doc for each source for detail. For example, [MQTT source](../guide/sources/builtin/mqtt.md) and [Neuron source](../guide/sources/builtin/neuron.md) covers the configuration items.
3. `etc/connections/connection.yaml`: shared connection configuration file.

## Configuration Methods

Users can set the configuration through 3 methods order by precedence.

1. Management Console/REST API
2. Environment variables
3. Yaml files in etc folder.

The yaml files usually be used to set up the default configurations. It can be heavily use when deploy in bare metal and the user can access the file system easily.

When deploying in docker or k8s, it is not easy enough to manipulate files, small amount of configurations can then be set or override by environment variables. And in runtime, end users will use management console to change the configurations dynamically. The `Configuration` page in the eKuiper manager can help users to modify the configurations visually.

### Environment variable syntax

There is a mapping from environment variable to the configuration yaml file. When modifying configuration through environment variables, the environment variables need to be set according to the prescribed format, for example:

```text
KUIPER__BASIC__DEBUG => basic.debug in etc/kuiper.yaml
MQTT_SOURCE__DEMO_CONF__QOS => demo_conf.qos in etc/mqtt_source.yaml
EDGEX__DEFAULT__PORT => default.port in etc/sources/edgex.yaml
CONNECTION__EDGEX__REDISMSGBUS__PORT => edgex.redismsgbus.port int etc/connections/connection.yaml
```

The environment variables are separated by "__", the content of the first part after the separation matches the file name of the configuration file, and the remaining content matches the different levels of the configuration items. The file name could be `KUIPER` and `MQTT_SOURCE` in the `etc` folder; or  `CONNECTION` in `etc/connection` folder. Otherwise, the file should in `etc/sources` folder.
