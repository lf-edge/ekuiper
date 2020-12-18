# 文件目标（Sink）

目标（Sink）用于将分析结果保存到指定文件中。

## 编译和部署插件

```shell
# cd $kuiper_src
# go build --buildmode=plugin -o plugins/sinks/File.so plugins/sinks/file/file.go
# cp plugins/sinks/File.so $kuiper_install/plugins/sinks
```

重新启动 Kuiper 服务器以激活插件。

## 属性

| 属性名称 | 是否可选 | 说明                                                         |
| -------- | -------- | ------------------------------------------------------------ |
| path     | 否       | 保存结果的文件路径，例如  `/tmp/result.txt`                  |
| interval | 是       | 写入分析结果的时间间隔（毫秒）。 默认值为1000，这表示每隔一秒钟写入一次分析结果。 |

## 使用示例

下面是一个选择温度大于50度的示例，每5秒将结果保存到文件 `/tmp/result.txt`  中。

```json
{
  "sql": "SELECT * from demo where temperature>50",
  "actions": [
    {
      "file": {
        "path": "/tmp/result.txt",
        "interval": 5000
      }
    }
  ]
}
```

