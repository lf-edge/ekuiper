# HTTP 接收源

eKuiper 为接收 HTTP 源流提供了内置支持，该支持可从 HTTP 客户端接收消息并输入 eKuiper 处理管道。 HTTP接收源的配置文件位于 `etc/sources/httppush.yaml`中。 以下是文件格式。

```yaml
#全局httppull配置
default:
  # 接收服务器地址
  server: ":8900" 
 

#重载全局配置
application_conf: #Conf_key
  server: ":9000"
```

## 全局HTTP接收配置

用户可以在此处指定全局 HTTP 接收设置。 `default` 部分中指定的配置项将用作所有HTTP 连接的默认设置。

### server

接收数据的服务器地址。



## 重载默认设置

如果您有特定的连接需要重载默认设置，则可以创建一个自定义部分。 在上一个示例中，我们创建了一个名为 `application_conf` 的特定设置。 然后，您可以在创建流定义时使用选项 `CONF_KEY` 指定配置（有关更多信息，请参见 [流规格](../../../sqls/streams.md)）。

**样例**

```
demo (
		...
	) WITH (DATASOURCE="/feed", FORMAT="JSON", TYPE="httppush", KEY="USERID", CONF_KEY="application_conf");
```

这些特定设置所使用的配置键与 `default` 设置中的配置键相同，在特定设置中指定的任何值都将重载 `default` 部分中的值。

