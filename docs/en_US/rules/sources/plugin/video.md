# Video Source

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

The source will query video streams such as RTSP encoded stream by `ffmpeg` command to get images.

## Compile & deploy plugin

```shell
# cd $eKuiper_src
# go build -trimpath -modfile extensions.mod --buildmode=plugin -o plugins/sources/Video.so extensions/sources/video/video.go
# cp plugins/sources/Video.so $eKuiper_install/plugins/sources
# cp plugins/sources/video.json $eKuiper_install/etc/sources
# cp plugins/sources/video.yaml $eKuiper_install/etc/sources
```

Restart the eKuiper server to activate the plugin.

## Configuration

The configuration for this source is `$ekuiper/etc/sources/video.yaml`. The format is as below:

```yaml
default:
  url: http://localhost:8080
  interval: 1000

ext:
  interval: 10000

dedup:
  interval: 100

```
### Global configurations

Use can specify the global video source settings here. The configuration items specified in `default` section will be taken as default settings for the source when running this source.

### url

The url address for the video streaming.

### interval

The interval (ms) to issue a message.


## Override the default settings

If you have a specific connection that need to overwrite the default settings, you can create a customized section. In the previous sample, we create a specific setting named with `ext`.  Then you can specify the configuration with option `CONF_KEY` when creating the stream definition (see [stream specs](../../../sqls/streams.md) for more info).

## Sample usage

```
demo (
		...
	) WITH (FORMAT="JSON", CONF_KEY="ext", TYPE="video");
```

The configuration keys "ext" will be used.

