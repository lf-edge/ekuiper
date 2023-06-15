# 图像目标（Sink）

目标（Sink）用于将图片保存到指定文件夹中。

## 编译和部署插件

```shell
# cd $eKuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/Image.so extensions/sinks/image/image.go
# cp plugins/sinks/Image.so $eKuiper_install/plugins/sinks
```

重新启动 eKuiper 服务器以激活插件。

## 属性

| 属性名称     | 是否可选 | 说明                                                    |
|----------|------|-------------------------------------------------------|
| path     | 否    | 保存图片的文件夹名，例如 `./tmp`。注意：多条 rule 路径不能重复，否则会出现彼此删除的现象。 |
| format   | 否    | 文件格式，支持 jpeg 和 png。                                   |
| maxAge   | 是    | 最长文件存储时间(小时)。默认值为72，这表示图片最多保存3天。                      |
| maxCount | 是    | 存储图片的最大数量，默认值是1000，删除时间较早的图片，与 `maxAge` 是或的关系。          |

## 使用示例

下面示例演示接收图片并将图片保存到文件夹 `/tmp`  中，当图片数量超过1000张时，删除时间较早的图片；图片保存时长超过 72 小时后，将删除超时的图片。

```json
{
  "sql": "SELECT * from demo",
  "actions": [
    {
      "image": {
        "path": "/tmp",
        "format": "png",
        "maxCount":1000,
        "maxage":72
      }
    }
  ]
}
```

## 演示

下面以 `zmq` 插件为 `source`，`image` 插件为 `sink`，将 `zmq` 接受到的图片保存在 `image` 指定的文件夹中。

```shell
curl http://127.0.0.1:9081/streams -X POST -d '{"sql":"create stream s(image bytea)WITH(DATASOURCE = \"\",FORMAT=\"binary\", TYPE=\"zmq\");"}'

curl http://127.0.0.1:9081/rules -X POST -d '{"id":"r","sql":"SELECT * FROM s","actions":[{"image":{"path":"./tmp","format":"png"}}]}'
```
