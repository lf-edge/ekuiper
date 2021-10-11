# 插件管理

eKuiper 插件命令行工具使您可以管理插件，例如创建，显示和删除插件。 请注意，删除插件将需要重新启动eKuiper 才能生效。 要更新插件，请执行以下操作：
1. 删除插件。
2. 重新启动 eKuiper。
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
# bin/kuiper create plugin source random {"file":"http://127.0.0.1/plugins/sources/random.zip"}
```

该命令创建一个名为 `random` 的源插件。

- 在文件中指定插件定义。 如果插件很复杂，或者插件已经以规范有序的格式写在文本文件中，则只需通过 `-f` 选项指定插件定义即可。

示例：

```shell
# bin/kuiper create plugin sink plugin1 -f /tmp/plugin1.txt
```

以下是 `plugin1.txt` 的内容。

```json
{
  "file":"http://127.0.0.1/plugins/sources/random.zip"
}
```

如果函数插件导出多个函数，则需要声明所有导出的函数名。

```shell
# bin/kuiper create plugin function mulfuncs "{\"file\":\"file:///tmp/kuiper/plugins/functions/mulfuncs.zip\",\"functions\":[\"func1\",\"func2\"]}"}
```

### 参数
1. plugin_type：插件类型，可用值为 `["source", "sink", "function", "portable"]`
2. plugin_name：插件的唯一名称。名称首字母必须小写。例如，如果导出的插件名称为 `Random`，则此插件的名称为 `Random`。
3. file：插件文件的网址。 它必须是一个 zip 文件，其中包含：编译后的 so 文件和 yaml 文件（仅源文件需要）。 文件名称必须与插件名称匹配。 关于命名规则，查看 [扩展名](../extension/overview.md) 。
4. functions：仅用于导出多个函数的函数插件。该参数指明插件导出的所有函数名。

## 显示插件

该命令用于显示服务器中为插件类型定义的所有插件。

```shell
show plugins function
```

示例：

```shell
# bin/kuiper show plugins function
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
# bin/kuiper describe plugin source plugin1
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
其中，`-s $stop` 是可选的布尔参数。 如果将其设置为 true，则 eKuiper 服务器将停止，以使删除生效。 用户将需要手动重新启动它。

示例：

```shell
# bin/kuiper drop plugin source random
Plugin random is dropped.
```

## 用于导出多函数的函数插件的相关命令

与 source 和 sink 插件不同，函数插件可以在一个插件里导出多个函数。导出的函数名必须全局唯一，不能与其他插件导出的函数同名。插件和函数是一对多的关系。因此，我们提供了 show udf （用户定义的函数） 命令用于查询所有已定义的函数名以便用户避免重复名字。我们也提供了 describe udf 命令，以便查询出定义该函数的插件名称。另外，我们提供了函数注册命令，用于给自动载入的函数注册导出的多个函数。

### 显示用户自定义函数列表

此命令用于展示所有自定义的函数的名称。 

```shell
show udfs
```

### 描述用户自定义函数

此命令用于展示定义此用户自定义函数的插件名称。

```shell
describe udf $udf_name
```

输出示例：

```json
{
  "name": "funcName",
  "plugin": "pluginName"
}
```

### 注册函数

此命令用于给自动载入的函数插件注册其导出的所有函数或者用于更改插件导出的函数列表。如果插件是经由命令行的创建命令或者 REST API 创建，且创建时提供了 functions 参数，则无需再执行此命令除非用于更改导出函数。此命令将会持久化到 KV 中。因此，除非需要更改导出函数列表，用户仅需执行注册函数一次。

```shell
register plugin function $pluginName "{\"functions\":[\"$funcName\",\"$anotherFuncName\"]}"
```

样例：

```shell
# bin/kuiper register plugin function myPlugin "{\"functions\":[\"func1\",\"func2\",\"funcn\"]}"
```