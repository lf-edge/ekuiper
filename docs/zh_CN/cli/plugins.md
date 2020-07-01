# 插件管理

Kuiper 插件命令行工具使您可以管理插件，例如创建，显示和删除插件。 请注意，删除插件将需要重新启动kuiper 才能生效。 要更新插件，请执行以下操作：
1. 删除插件。
2. 重新启动 Kuiper。
3. 使用新配置创建插件。

## 创建插件

该命令用于创建插件。 插件的定义以 JSON 格式指定。

```shell
create plugin $plugin_type $plugin_name $plugin_json | create plugin $plugin_type $plugin_name -f $plugin_def_file
```

插件可以通过两种方式创建。

- 在命令行中指定插件定义。

示例：

```shell
# bin/cli create plugin source random {"file":"http://127.0.0.1/plugins/sources/random.zip"}
```

该命令创建一个名为 `random` 的源插件。

- 在文件中指定插件定义。 如果插件很复杂，或者插件已经以规范有序的格式写在文本文件中，则只需通过 `-f` 选项指定插件定义即可。

示例：

```shell
# bin/cli create plugin sink plugin1 -f /tmp/plugin1.txt
```

以下是 `plugin1.txt` 的内容。

```json
{
  "file":"http://127.0.0.1/plugins/sources/random.zip"
}
```
### 参数
1. plugin_type：插件类型，可用值为 `["source", "sink", "functions"]`
2. plugin_name：插件的唯一名称。名称首字母必须小写。例如，如果导出的插件名称为 `Random`，则此插件的名称为 `Random`。
3. file：插件文件的网址。 它必须是一个 zip 文件，其中包含：编译后的 so 文件和 yaml 文件（仅源文件需要）。 文件名称必须与插件名称匹配。 关于命名规则，查看 [扩展名](../extension/overview.md) 。

## 显示插件

该命令用于显示服务器中为插件类型定义的所有插件。

```shell
show plugins function
```

示例：

```shell
# bin/cli show plugins function
function1
function2
```

## 描述插件
该命令用于打印插件的详细定义。

```shell
describe plugin $plugin_type $plugin_name
```

示例：

```shell
# bin/cli describe plugin source plugin1
{
  "name": "plugin1",
  "version": "1.0.0"
}
```

## 删除插件

该命令用于删除插件。

```shell
drop plugin $plugin_type $plugin_name -s $stop 
```
其中，`-s $stop` 是可选的布尔参数。 如果将其设置为 true，则 Kuiper 服务器将停止，以使删除生效。 用户将需要手动重新启动它。

示例：

```shell
# bin/cli drop plugin source random
Plugin random is dropped.
```