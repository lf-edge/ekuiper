# dirwatch 数据源

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>

eKuiper 内置支持 dirwatch 数据源。通过监视对应 PATH 的文件目录，读取文件目录中的文件数据。当对应文件目录中的文件被创建、修改时，eKuiper 将会读取该文件。

## 配置

eKuiper 连接器可以通过[环境变量](../../../configuration/configuration.md#environment-variable-syntax)、[REST API](../../../api/restapi/configKey.md) 或配置文件进行配置，本节将介绍配置文件的使用方法。

MQTT 源连接器的配置文件位于：`$ekuiper/etc/sources/dirwatch.yaml`，其中：

以下示例包括一个全局配置和自定义配置 `demo_conf`：

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

## 全局配置

用户可在 `default` 部分指定全局设置。

### 相关配置

- `path`：监视对应的 PATH 文件目录
- `allowedExtension`：支持读取的文件后缀名，若没有定义，则支持读取所有文件的后缀名

## 数据结构

当对应文件目录中的文件被创建、修改时，eKuiper 将会读取该文件，dirwatch 将会如下构造数据结构:

```json
{
  "content":"MTIz",             // 文件内容 []byte base64 之后的结果
  "filename":"test.txt",        // 文件名
  "modifyTime":1732241987       // 文件的修改时间
}
```
