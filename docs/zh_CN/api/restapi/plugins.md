eKuiper REST api 允许您管理插件，例如创建、删除和列出插件。 请注意，删除插件将需要重新启动 eKuiper 才能生效。 要更新插件，请执行以下操作：

1. 删除插件。
2. 重新启动 eKuiper。
3. 使用新配置创建插件。

## 创建插件

该API接受JSON内容以创建新的插件。 每种插件类型都有一个独立的端点。 支持的类型为 `["源", "目标", "函数", "便捷插件"]`。 插件由名称标识。 名称必须唯一。

```shell
POST http://localhost:9081/plugins/sources
POST http://localhost:9081/plugins/sinks
POST http://localhost:9081/plugins/functions
POST http://localhost:9081/plugins/portables
```
文件在http服务器上时的请求示例：

```json
{
  "name":"random",
  "file":"http://127.0.0.1/plugins/sources/random.zip"
}
```

文件在eKuiper所在服务器上时的请求示例：
```json
{
  "name":"random",
  "file":"file:///var/plugins/sources/random.zip"
}
```

### 参数

1. name：插件的唯一名称。 名称必须采用首字母小写的驼峰命名法。 例如，如果导出的插件名称为 `Random`，则此插件的名称为 `random`。
2. file：插件文件的 URL。URL 支持 http 和 https 以及 file 模式。当使用 file 模式时，该文件必须在 eKuiper 服务器所在的机器上。它必须是一个 zip 文件，其中包含：编译后的 so 文件和yaml 文件（仅源必需）。 如果插件依赖于某些外部依赖项，则可以提供一个名为install.sh 的 bash 脚本来进行依赖项安装。 文件名称必须与插件名称匹配。 请参考 [扩展](../../extension/overview.md) 了解命名规则。

### 插件文件格式
`注意`：针对`便捷插件`类型的文件格式，请参考这篇[文章](../../extension/portable/overview.md#打包发布) 

名为 random.zip 的源的示例 zip 文件
1. Random@v1.0.0.so
2. random.yaml
3. install.sh
4. install.sh 的各种依赖文件/文件夹
   - mysdk.zip
   - myconfig.conf
5. etc 目录：插件的运行时配置文件或依赖文件。插件安装后，该目录将重名为插件名并复制到 {{eKuiperPath}}/etc/{{pluginType}} 目录下。

请注意，将在系统可能已经具有库或软件包的情况下运行 install.sh。 确保在运行之前检查路径。 下面是一个示例 install.sh，用于安装示例 sdk 库。 

```bash
#!/bin/sh
dir=/usr/local/mysdk
cur=$(dirname "$0")
echo "Base path $cur" 
if [ -d "$dir" ]; then
    echo "SDK path $dir exists." 
else
    echo "Creating SDK path $dir"
    mkdir -p $dir
    echo "Created SDK path $dir"
fi

apt install --no-upgrade unzip
if [ -d "$dir/lib" ]; then
    echo "SDK lib path $dir/lib exists." 
else
    echo "Unzip SDK lib to path $dir"
    unzip $cur/mysdk.zip -d $dir
    echo "Unzipped SDK lib to path $dir"
fi

if [ -f "/etc/ld.so.conf.d/myconfig.conf" ]; then
    echo "/etc/ld.so.conf.d/myconfig.conf exists"
else
    echo "Copy conf file"
    cp $cur/myconfig.conf /etc/ld.so.conf.d/
    echo "Copied conf file"
fi
ldconfig
echo "Done"
```

## 显示插件

该 API 用于显示服务器中为插件类型定义的所有插件。

```shell
GET http://localhost:9081/plugins/sources
GET http://localhost:9081/plugins/sinks
GET http://localhost:9081/plugins/functions
GET http://localhost:9081/plugins/portables
```

响应示例：

```json
["plugin1","plugin2"]
```

## 描述插件

该 API 用于打印插件的详细定义。

```shell
GET http://localhost:9081/plugins/sources/{name}
GET http://localhost:9081/plugins/sinks/{name}
GET http://localhost:9081/plugins/functions/{name}
GET http://localhost:9081/plugins/portables/{name}
```

路径参数 `name` 是插件的名称。

响应示例：

```json
{
  "name": "plugin1",
  "version": "1.0.0"
}
```

## 删除插件

该 API 用于删除插件。 需要注意的是，对于原生插件，删除操作需要重启 eKuiper 服务器才能生效。这意味着运行中的规则仍然会使用已删除的插件正常运行，直到重启。对于 portable 插件，删除操作立即生效。使用插件的规则仍然处于运行状态，但可能会收到错误。当有同名的 Portable 插件创建时，这些规则将自动使用新的插件运行。如果不希望规则保持运行，需要在删除插件之前，手动删除使用插件的规则。

```shell
DELETE http://localhost:9081/plugins/sources/{name}
DELETE http://localhost:9081/plugins/sinks/{name}
DELETE http://localhost:9081/plugins/functions/{name}
DELETE http://localhost:9081/plugins/portables/{name}
```
用户可以传递查询参数来决定是否应在删除后停止 eKuiper，以使删除生效。 参数是`stop`，只有当值是1时，eKuiper 才停止。 用户必须手动重新启动它。

```shell
DELETE http://localhost:9081/plugins/sources/{name}?stop=1
```

## 更新插件

仅 portable 插件支持更新操作。正在使用插件的规则将自动热加载新的插件实现。

```shell
PUT http://localhost:9081/plugins/portables/{name}
```

## 用于导出多函数的函数插件的相关 API

与 source 和 sink 插件不同，函数插件可以在一个插件里导出多个函数。导出的函数名必须全局唯一，不能与其他插件导出的函数同名。插件和函数是一对多的关系。因此，我们提供了 show udf （用户定义的函数） 接口用于查询所有已定义的函数名以便用户避免重复名字。我们也提供了 describe udf 接口，以便查询出定义该函数的插件名称。另外，我们提供了函数注册接口，用于给自动载入的函数注册导出的多个函数。

### 显示用户自定义函数列表

该 API 用于展示所有自定义的函数的名称。

```shell
GET http://localhost:9081/plugins/udfs
```

结果样例：

```json
["func1","func2"]
```

### 描述用户自定义函数

该 API 用于展示定义此用户自定义函数的插件名称。

```shell
GET http://localhost:9081/plugins/udfs/{name}
```

结果样例：

```json
{
  "name": "funcName",
  "plugin": "pluginName"
}
```

### 注册函数

该 API 用于给自动载入的函数插件注册其导出的所有函数或者用于更改插件导出的函数列表。如果插件是经由命令行的创建命令或者 REST API 创建，且创建时提供了 functions 参数，则无需再执行此命令除非用于更改导出函数。此命令将会持久化到 KV 中。因此，除非需要更改导出函数列表，用户仅需执行注册函数一次。

```shell
POST http://{{host}}/plugins/functions/{plugin_name}/register

{"functions":["func1","func2"]}

```

## 获取可安装的插件

根据在 `etc/kuiper.yaml` 文件中 `pluginHosts` 的配置，获取适合本 eKuiper 实例运行的插件列表，缺省会从 `https://packages.emqx.net` 上去获取。

```
GET http://localhost:9081/plugins/sources/prebuild
GET http://localhost:9081/plugins/sinks/prebuild
GET http://localhost:9081/plugins/functions/prebuild
```

样例返回内容如下，其中键值为插件名称，值是插件的下载地址。

```json
{
  "file": "http://127.0.0.1:63767/kuiper-plugins/0.9.1/sinks/alpine/file_arm64.zip",
  "influx": "http://127.0.0.1:63767/kuiper-plugins/0.9.1/sinks/alpine/influx_arm64.zip",
  "zmq": "http://127.0.0.1:63768/kuiper-plugins/0.9.1/sinks/alpine/zmq_arm64.zip"
}
```