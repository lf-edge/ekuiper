The eKuiper REST api for configuration file uploads allows you to upload configuration files and list all uploaded files.

## Upload a configuration file

The API accepts a multipart file upload requests. Below is an example html file to upload file to `http://127.0.0.1:9081/config/uploads`. In the form data, the file input name must be `uploadFile`.

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

The upload request will save the file into your `${configPath}/uploads`. It will override the existed file of the same name. The response is the absolute path of the uploaded file which you can refer in other configurations.

## Show uploaded file list

The API is used for displaying all files in the `${configPath}/uploads` path.

```shell
GET http://localhost:9081/config/uploads
```

Response Sample:

```json
[
   "/ekuiper/etc/uploads/zk.gif",
   "/ekuiper/etc/uploads/abc.gif"
]
```