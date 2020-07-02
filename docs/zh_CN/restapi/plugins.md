Kuiper REST api 允许您管理插件，例如创建、删除和列出插件。 请注意，删除插件将需要重新启动 kuiper 才能生效。 要更新插件，请执行以下操作：

1. 删除插件。
2. 重新启动 Kuiper。
3. 使用新配置创建插件。

## 创建插件

该API接受JSON内容以创建新的插件。 每种插件类型都有一个独立的端点。 支持的类型为 `["源", "目标", "函数"]`。 插件由名称标识。 名称必须唯一。

```shell
POST http://localhost:9081/plugins/sources
POST http://localhost:9081/plugins/sinks
POST http://localhost:9081/plugins/functions
```
请求示例：

```json
{
  "name":"random",
  "file":"http://127.0.0.1/plugins/sources/random.zip"
}
```

### 参数

1. name：插件的唯一名称。 名称必须采用首字母小写的驼峰命名法。 例如，如果导出的插件名称为 `Random`，则此插件的名称为 `random`。
2. file：插件文件的 URL。 它必须是一个 zip 文件，其中包含：编译后的 so 文件和yaml 文件（仅源必需）。 如果插件依赖于某些外部依赖项，则可以提供一个名为install.sh 的 bash 脚本来进行依赖项安装。 文件名称必须与插件名称匹配。 请参考[扩展](../extension/overview.md) 了解命名规则。

### 插件文件格式
名为 random.zip 的源的示例 zip 文件
1. Random@v1.0.0.so
2. random.yaml
3. install.sh
4. install.sh 的各种依赖文件/文件夹
   - mysdk.zip
   - myconfig.conf  

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

该 API 用于删除插件。 需要重启 kuiper 服务器才能生效。

```shell
DELETE http://localhost:8080/plugins/sources/{name}
DELETE http://localhost:8080/plugins/sinks/{name}
DELETE http://localhost:8080/plugins/functions/{name}
```
用户可以传递查询参数来决定是否应在删除后停止 Kuiper，以使删除生效。 参数是`restart`，只有当值是1时，Kuiper 才停止。 用户必须手动重新启动它。

```shell
DELETE http://localhost:8080/plugins/sources/{name}?restart=1
```