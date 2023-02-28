# Zmq 源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

源将订阅 Zero Mq 主题以将消息导入 eKuiper。

## 编译和部署插件

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sources/Zmq.so extensions/sources/zmq/zmq.go
# cp plugins/sources/Zmq.so $eKuiper_install/plugins/sources
```

重新启动 eKuiper 服务器以激活插件。

## 配置

该源的配置位于 `$ekuiper/etc/sources/zmq.yaml`。格式如下：

```yaml
#Global Zmq configurations
default:
  server: tcp://192.168.2.2:5563  
test:
  server: tcp://127.0.0.1:5563
```
### 全局配置

用户可以在此处指定全局 zmq 源设置。 连接到Zero Mq 时，`default` 部分中指定的配置项目将被用作源的默认设置。

### server

源将订阅的Zero Mq 服务器的 URL。

## 覆盖默认设置

如果您有特定的连接需要覆盖默认设置，则可以创建一个自定义部分。 在上一个示例中，我们创建一个名为 `test` 的特定设置。 然后，您可以在创建流定义时使用选项 `CONF_KEY` 指定配置（有关更多信息，请参见 [stream specs](../../../sqls/streams.md)）。

## 使用示例

```
demo (
		...
	) WITH (DATASOURCE="demo", FORMAT="JSON", CONF_KEY="test", TYPE="zmq");
```

将使用配置键 "test"。 订阅的 Zero Mq 主题是 `DATASOURCE` 中指定的 "demo"。

