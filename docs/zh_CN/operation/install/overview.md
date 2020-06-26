# 安装说明

请下载安装程序包，有关不同操作系统的安装说明，请参阅以下内容。

- [Cent-OS](cent-os.md)
- ...

# 安装目录结构 

下面是安装后的目录结构。

```shell
bin
  cli
etc
  mqtt_source.yaml
  *.yaml
data
plugins
log
```

## bin

`bin`目录包含所有可执行文件。 如``cli''命令。

## etc

``etc``目录包含Kuiper的配置文件， 例如MQTT源配置等。

## data

Kuiper会持久化流和规则的所有定义，并且所有消息都将存储在此文件夹中，以进行长时间操作。

## plugins

Kuiper允许用户开发自己的插件，并将这些插件放入此文件夹。 有关如何扩展Kuiper的更多信息，请参见[扩展](../../extension/overview.md)。

## log

所有日志文件都在此文件夹下。 默认的日志文件名为``stream.log``。

# 下一步

- 参考 [入门指导](../../getting_started.md) 开始您的Kuiper体验。
- 参考 [CLI 工具](../../cli/overview.md) 了解Kuiper CLI工具的使用。

