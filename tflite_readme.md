# TFLite 函数使用说明

本包中的 `kuiperd` 已在编译时通过 `tflite` tag 内置 `tflite` 函数,无需替换二进制,
也无需安装任何系统库。运行库 `libtensorflowlite_c.so` 已包含在本包的 `lib/` 目录下,
只需在启动前设置 `LD_LIBRARY_PATH` 指向它即可。

## 启动方式

在解压后的包目录下执行:

```bash
export LD_LIBRARY_PATH=$PWD/lib:$LD_LIBRARY_PATH
./bin/kuiperd
```

- Linux: 直接使用,`kuiperd` 启动后即可使用 `tflite` 函数。
- Android: 需在 root shell 下执行上述命令,同样设置 `LD_LIBRARY_PATH` 后启动。

如需开机自启或后台运行,请把 `export LD_LIBRARY_PATH=$PWD/lib:$LD_LIBRARY_PATH`
一并写入你的启动脚本或 systemd/init 配置。

## 验证

以包内自带的 `sin_model.tflite` 为例(需先将模型放入 `data/uploads/` 目录,例如
通过 REST API `POST /config/uploads` 上传,或直接复制):

1. 创建数据流:

   ```http
   POST /streams
   {"sql": "CREATE STREAM tfdemo() WITH (TYPE=\"mqtt\", FORMAT=\"json\", DATASOURCE=\"tfdemo\")"}
   ```

2. 创建规则,第一个参数为模型名,第二个参数为输入数据(该模型输入为一维数组,如 [1.5]):

   ```json
   {
     "id": "tf1",
     "sql": "SELECT tflite(\"sin_model\", data) FROM tfdemo",
     "actions": [{"mqtt": {"server": "tcp://broker:1883", "topic": "result/tf", "qos": 1, "format": "json", "sendSingle": true}}]
   }
   ```

3. 向 `tfdemo` topic 发送 `{"data":[1.5]}`,应收到结果 `{"tflite": [[0.98928607]]}`。

## 模型

- 模型文件需放置在 `data/uploads/<模型名>.tflite`,规则中第一个参数使用不带后缀的模型名。
- 支持输入/输出类型: float32、int64、int32、int16、int8、uint8。
