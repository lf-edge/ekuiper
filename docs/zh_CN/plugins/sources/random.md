# 随机源

随机源将生成具有指定样式的随机输入。

## 编译和部署插件

```shell
# cd $kuiper_src
# go build --buildmode=plugin -o plugins/sources/Random.so plugins/sources/random/random.go
# cp plugins/sources/Random.so $kuiper_install/plugins/sources
```

重新启动 Kuiper 服务器以激活插件。

## 配置

该源的配置为 `$kuiper/etc/sources/random.yaml`。格式如下：

```yaml
default:
  interval: 1000
  seed: 1
  pattern:
    count: 50
  deduplicate: 0

ext:
  interval: 100

dedup:
  interval: 100
  deduplicate: 50
```
### 全局配置

用户可以在此处指定全局随机源设置。 运行此源时，将在 `default` 部分中指定的配置项目作为源的默认设置。

### interval

发出消息的间隔（毫秒）。

### seed

随机函数产生的最大整数。

### pattern

源生成的样式。 在上面的示例中，样式将为 json，例如{"count":50}

### deduplicate

一个整数值。 如果它为正数，则源不会发出与以前任何“重复数据删除”长度的消息重复的消息。如果为0，则源不会检查是否存在重复。如果是负数，则源将检查以前任何消息的重复项。如果有非常大的输入数据集，请不要使用负长度，因为将保留所有以前的数据。

## 覆盖默认设置

如果您有特定的连接需要覆盖默认设置，则可以创建一个自定义部分。 在上一个示例中，我们创建一个名为 `test` 的特定设置。 然后，您可以在创建流定义时使用选项`CONF_KEY` 指定配置（有关更多信息，请参见 [stream specs](../../sqls/streams.md)）。

## 使用示例

```
demo (
		...
	) WITH (DATASOURCE="demo", FORMAT="JSON", CONF_KEY="ext", TYPE="random");
```

配置键 "ext" 将被使用。

