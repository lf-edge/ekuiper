# Scripts management

The command line tools allows you to manage UDFs aka. scripts, such as create, show, drop, describe scripts. Currently, only JavaScript function is supported.

## Register a script

The command is used for creating a JavaScript function. The function's definition is specified with JSON format

```shell
create script $script_json
```

Example：

```shell
# bin/kuiper create script "{\"id\": \"area\",\"description\": \"calculate the area\",\"script\": \"function area(x, y) { return x * y; }\",\"isAgg\": false}"
```

This command creates a JavaScript function named area. The JSON object is with the following fields:

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

## Show All Scripts

The command is used for describing all JavaScript functions defined in the server.

```shell
# bin/kuiper show scripts
```

The response will be a list of function names. For example:

```json
["area"]
```

## Describe a Script

The command prints the detailed definition of a JavaScript function.

```shell
describe script $script_name
```

Example：

```shell
# bin/kuiper describe area
{
   "id": "area",
   "description": "calculate area",
   "script": "function area(x, y) { return x * y; }",
   "isAgg": false
}
```

## Delete a Script

The command drops the JavaScript function.

```shell
drop service $script_name
```

Example：

```shell
# bin/kuiper drop script area
```
