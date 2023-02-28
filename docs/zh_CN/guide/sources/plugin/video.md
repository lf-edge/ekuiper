# 视频源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

视频源会通过 `ffmpeg` 命令查询视频流，例如 RTSP 视频流获取图片

## 编译和部署插件

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sources/Video.so extensions/sources/video/video.go
# cp plugins/sources/Video.so $eKuiper_install/plugins/sources
# cp plugins/sources/video.json $eKuiper_install/etc/sources
# cp plugins/sources/video.yaml $eKuiper_install/etc/sources
```

重新启动 eKuiper 服务器以激活插件。

## 配置

该源的配置为 `$ekuiper/etc/sources/video.yaml`。格式如下：

```yaml
default:
  url: http://localhost:8080
  interval: 1000

ext:
  interval: 10000

dedup:
  interval: 100
```
### 全局配置

用户可以在此处指定全局随机源设置。 运行此源时，将在 `default` 部分中指定的配置项目作为源的默认设置。

### url

视频源地址

### interval

发出消息的间隔（毫秒）。


## 覆盖默认设置

如果您有特定的连接需要覆盖默认设置，则可以创建一个自定义部分。 在上一个示例中，我们创建一个名为 `ext` 的特定设置。 然后，您可以在创建流定义时使用选项`CONF_KEY` 指定配置（有关更多信息，请参见 [stream specs](../../../sqls/streams.md)）。

## 使用示例

```
demo (
		...
	) WITH (FORMAT="JSON", CONF_KEY="ext", TYPE="video");
```

配置键 "ext" 将被使用。

