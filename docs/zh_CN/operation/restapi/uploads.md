eKuiper REST api允许您上传配置文件并列出所有上传的文件。

## 上传配置文件

该API接受多部分的文件上传请求。下面是一个上传文件到 `http://127.0.0.1:9081/config/uploads` 的 html 文件例子。在表格数据中，文件输入名称必须是 `uploadFile` 。

```html
<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta http-equiv="X-UA-Compatible" content="ie=edge" />
    <title>Upload File</title>
  </head>
  <body>
    <form
      enctype="multipart/form-data"
      action="http://127.0.0.1:9081/config/uploads"
      method="post"
    >
      <input type="file" name="uploadFile" />
      <input type="submit" value="upload" />
    </form>
  </body>
</html>
```

上传请求将把文件保存到你的 `${configPath}/uploads` 。它将覆盖现有的同名文件。返回的响应是上传文件的绝对路径，从而可以在其他配置中使用。

## 获取上传文件的列表

该API用于显示 `${configPath}/uploads` 路径中的所有文件。

```shell
GET http://localhost:9081/config/uploads
```

响应示例：

```json
[
   "/ekuiper/etc/uploads/zk.gif",
   "/ekuiper/etc/uploads/abc.gif"
]
```