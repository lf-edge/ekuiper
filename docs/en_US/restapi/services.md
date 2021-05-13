Kuiper REST api allows you to manage external services, such as registering, deleting and listing services, listing external functions.

## Register external services

This API accepts JSON content to create new external services.

```shell
POST http://localhost:9081/services
```
An example of a request for a file on an HTTP server:

```json
{
  "name":"random",
  "file":"http://127.0.0.1/services/sample.zip"
}
```

An example of a request for a file on the Kuiper server:
```json
{
  "name":"random",
  "file":"file:///var/services/sample.zip"
}
```

### parameter

1. name: The unique name of the external service, which must be exactly the same as the json file of service definition in the zip file.
2. file: URL of external service file. URL supports http, https and file modes. When using the file mode, the file must be on the machine where the Kuiper server is located. It must be a zip file, which contains the service description json file with the same name as the service and any other auxiliary files. The schema file must be in the schema folder.

### Service file format
A sample zip file of the source named sample.zip
1. sample.json
2. Schema directory: it contains one or more schema files used by the service. For example, sample.proto.


## Display external services

This API is used to display all external services defined in the server.

```shell
GET http://localhost:9081/services
```

Response example:

```json
["sample","sample2"]
```

## Describe external services

This API is used to print detailed definitions of external services.

```shell
GET http://localhost:9081/services/{name}
```

The path parameter `name` is the name of the external service.

## Delete external services

This API is used to delete external services, and all functions defined under the service will be deleted.

```shell
DELETE http://localhost:8080/services/{name}
```

## Update external services

This API is used to update external services, and its parameters are the same as that of service registration.

```shell
PUT http://localhost:9081/services/{name}

{
  "name":"random",
  "file":"http://127.0.0.1/services/sample.zip"
}
```

## Display all external functions

Each service can contain multiple functions. This API is used to display the names of all external functions that can be used in SQL.

```shell
GET http://localhost:9081/services/functions
```

Result example:

```json
["func1","func2"]
```

### Describe external functions

This API is used to display the name of the service that defines this external function.

```shell
GET http://localhost:9081/services/functions/{name}
```

Result example:

```json
{
  "name": "funcName",
  "serviceName": "serviceName"
}
```