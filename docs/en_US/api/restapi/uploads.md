The eKuiper REST api for configuration file uploads allows you to upload configuration files and list all uploaded files.

## Upload a configuration file

The API supports to upload a local file or provide the text content of file. The upload request will save the file into your `${dataPath}/uploads`. It will override the existed file of the same name. The response is the absolute path of the uploaded file which you can refer in other configurations.

### Upload by a file

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

### Upload by content

Provide the text content and file name to create a configuration file.

```shell
POST http://localhost:9081/config/uploads

{
  "name": "my.json",
  "content": "{\"hello\":\"world\"}"
}
```

## Show uploaded file list

The API is used for displaying all files in the `${dataPath}/uploads` path.

```shell
GET http://localhost:9081/config/uploads
```

Response Sample:

```json
[
   "/ekuiper/data/uploads/zk.gif",
   "/ekuiper/data/uploads/abc.gif"
]
```

## Delete an uploaded file

The API is used for deleting a file in the `${dataPath}/uploads` path.

```shell
DELETE http://localhost:9081/config/uploads/{fileName}
```
