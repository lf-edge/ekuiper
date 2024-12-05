# dirwatch Source Connector

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>

eKuiper has built-in support for the dirwatch data source. Read the file data in the file directory by monitoring the file directory corresponding to PATH. When a file in the corresponding file directory is created or modified, eKuiper will read the file.

## Configurations

The connector in eKuiper can be configured with [environment variables](../../../configuration/configuration.md#environment-variable-syntax), [rest API](../../../api/restapi/configKey.md), or configuration file. This section focuses on configuring eKuiper connectors with the configuration file.

eKuiper's default MQTT source configuration resides at `$ekuiper/etc/sources/dirwatch.yaml`.

See below for a demo configuration with the global configuration and a customized `demo_conf` section.

```yaml
default:
  path: /example
  allowedExtension:
    - txt

demo_conf: #Conf_key
  path: /example
  allowedExtension:
    - txt
```

## Global configuration

Users can specify global settings in the `default` section.

### Related configuration

- `path`: monitor the corresponding PATH file directory
- `allowedExtension`: Supports reading file suffixes. If not defined, supports reading suffixes of all files.

## Data structure

When a file in the corresponding file directory is created or modified, eKuiper will read the file, and dirwatch will construct the data structure as follows:

```json
{
  "content":"MTIz",         // The result after file content []byte base64
  "filename":"test.txt",    // file name
  "modifyTime":1732241987   // Modification time of the file
}
```
