# Image Sink

Sink is used to save the picture to the specified folder.

## Compile and deploy the plugin

```shell
# cd $kuiper_src
# go build -trimpath --buildmode=plugin -o plugins/sinks/Image.so plugins/sinks/image/image.go
# cp plugins/sinks/Image.so $kuiper_install/plugins/sinks
```

Restart the Kuiper server to activate the plugin.

## Attribute

| Attribute name | Optional | Description                                                  |
| -------------- | -------- | ------------------------------------------------------------ |
| path           | False    | The name of the folder where the pictures are saved, such as `./tmp`. Note: For multiple rules, their paths cannot be repeated, otherwise they will be deleted from each other. |
| format         | False    | File format, support jpeg and png.                           |
| maxAge         | True     | Maximum file storage time (hours). The default value is 72, which means that the picture can be stored for up to 3 days. |
| maxCount       | True     | The maximum number of stored pictures. The default value is 1000. The earlier pictures will be deleted. The relationship with `maxAge` is OR. |

## Usage example

The following example demonstrates how to receive pictures and save them to the folder `/tmp`. When the number of pictures exceeds 1000, the earlier pictures will be deleted. When the pictures are saved for more than 72 hours, the timeout pictures will be deleted.

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

## Demo

In the following example, we take the `zmq` plugin as `source` and the `image` plugin as `sink`, and save the pictures received by `zmq` in the folder specified by `image`.

```shell
curl http://127.0.0.1:9081/streams -X POST -d '{"sql":"create stream s(image bytea)WITH(DATASOURCE = \"\",FORMAT=\"binary\", TYPE=\"zmq\");"}'

curl http://127.0.0.1:9081/rules -X POST -d '{"id":"r","sql":"SELECT * FROM s","actions":[{"image":{"path":"./tmp","format":"png"}}]}'
```