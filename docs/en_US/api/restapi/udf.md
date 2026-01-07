# User-Defined Functions (UDF) Management API

Besides defining function in [plugins](./plugins.md), user-defined functions (UDF) are also supported independently. Currently, we only support JavaScript UDF. We can use REST API or [CLI](../cli/scripts.md) to manage JavaScript functions. You can create, list, describe, delete, and update functions.

## Create a UDF

Use this endpoint to create a new function.

```shell
POST http://localhost:9081/udf/javascript
```

The request body should be a JSON object with the following fields:

- id: A unique name for the function. This name must also be defined as a function in the script field.
- description: A brief description of the function.
- script: The function implementation in JavaScript.
- isAgg: A boolean indicating whether the function is an aggregate function.

Here's an example:

```json
{
  "id": "area",
  "description": "calculate area",
  "script": "function area(x, y) { return x * y; }",
  "isAgg": false
}
```

## List UDFs

Use this endpoint to display all JavaScript functions defined in the server.

```shell
GET http://localhost:9081/udf/javascript
```

The response will be a list of function names. For example:

```json
["area"]
```

## Describe a UDF

Use this endpoint to get the detailed definition of a function.

```shell
GET http://localhost:9081/udf/javascript/{id}
```

Replace {id} with the name of the function you want to describe. The response will be a JSON object with the function's details. For example:

```json
{
  "id": "area",
  "description": "calculate area",
  "script": "function area(x, y) { return x * y; }",
  "isAgg": false
}
```

## Delete a UDF

Use this endpoint to delete a function.

```shell
DELETE http://localhost:9081/udf/javascript/{id}
```

Replace {id} with the name of the function you want to delete. Note that you need to manually stop or delete any rules using the UDF before deleting it. A running rule will not be affected by the deletion of a UDF.

## Update a UDF

The JavaScript UDF can be updated and hot reload. Notice that, a running rule must be restarted to load the updated function.

```shell
PUT http://localhost:9081/udf/javascript/{id}
```

Replace {id} with the name of the function you want to update. The request body should be the same as when creating a UDF. If the function of the id does not exist, it will be created. Otherwise, it will be updated.
