# Script Functions

Script functions provide a mechanism for quickly extending functionality. Compared to plugin development, the development and operation costs of script functions are lower. Script functions do not need to be compiled and packaged, they can be registered and used directly with plain text, and can be quickly exported and imported in the same way as rules when upgrading versions or changing deployment nodes.

The system currently supports JavaScript as the scripting language.

> Notice:
> The script functions are not included in the default (aka. alpine) docker image and precompiled binary. If you want to
> use the script functions, please use the slim or slim-python docker image, or full version binary. To compile with the
> script functions, you need to add the `script` build tag.

## JavaScript Functions

The software has a built-in JavaScript interpreter [goja](https://github.com/dop251/goja), which allows you to write script functions directly in JavaScript. Due to the large built-in interpreter, the default compiled version does not include the script function extension. Users need to use the precompiled full version or slim version docker image. When compiling yourself, you need to add the `script` build tag.

The general steps for users to write script functions in JavaScript are as follows:

1. Write and debug JavaScript functions
2. Register the script function in eKuiper
3. Use the script function in SQL
4. Input the data stream and check the running result

### Writing JavaScript Functions

Users can use their favorite editor to write JavaScript script functions and debug by themselves. The script function needs to include the function definition and all dependent variables and other functions. After registration, the script function will be mapped to a SQL function, using the same function signature and getting the same return value type.

```javascript
function echo(msg) {
  return msg;
}
```

Please note that only the syntax and functions supported by [goja](https://github.com/dop251/goja) can be used in the function, that is, the ECMA 5.1 standard. Due to the weak type characteristics of JavaScript and the strong type characteristics of SQL, users should consider type conversion and verification when writing code.

#### Aggregate Functions

If the function needs to be used as an aggregate function, the user should expect the parameters to be an array and the return value to be a single value when writing the function. For example:

```javascript
function count_by_js(msgs) {
  return msg.length;
}
```

### Management of JavaScript Functions

After the function is written and debugged, the user needs to register the function in eKuiper. There are two ways to register:

1. Register using [REST API](../../api/restapi/udf.md)
2. Register using [CLI](../../api/cli/scripts.md)

When registering, you need to provide information such as the function name and function code text. After successful registration, it can be used in SQL. At the same time, you can view the information of the registered function through the REST API or CLI, and update or delete it.

### Use in SQL

In the current version, the registered function can be used directly in SQL. However, SQL does not provide static validation of function parameters and return values. Therefore, users need to ensure that the function parameters and return value types are consistent with the JavaScript function signature, or adapt different parameter types in the function implementation. Users can throw exceptions in JavaScript functions. Exceptions will be treated as runtime errors when running rules.

## Use Cases

Assuming that the user has completed the development of a JavaScript function for calculating the area, the following steps can be used to use it in the rule.

1. Register the function

   ```http request
   POST udf/javascript

   {
     "id": "area",
     "description": "calculate area",
     "script": "function area(x, y) { log(\"Hello, World!\"); return x * y;}",
     "isAgg": false
   }
   ```

2. Use in SQL, assuming there is an MQTT data stream `mqttDemo`, you can create the following rules:

   ```json
   {
     "id": "ruleArea",
     "sql": "SELECT area(length, width) FROM mqttDemo",
     "actions": [
       {
         "mqtt": {
           "server": "tcp://127.0.0.1:1883",
           "topic": "result/area",
           "sendSingle": true
         }
       }
     ]
   }
   ```

3. Input similar data through MQTT

   ```json
   { "length": 3, "width": 4 }
   ```

4. Subscribe to the MQTT topic `result/area`, check the continuous output results, for example

   ```json
   { "area": 21 }
   ```
