# 脚本函数

脚本函数提供了一种快速扩展功能的机制。相比于插件开发，脚本函数的开发和运维成本更低。脚本函数无需编译和打包，可以直接使用纯文本注册和使用，在版本升级的时候或变更部署节点是，可与规则相同的方式快速导出导入。

目前系统支持的脚本语言为 JavaScript。

> 注意:
> 默认的 Docker image（即 alpine 版本）以及默认的预编译二进制版本未包含脚本函数功能。若要使用这个功能，请使用 slim 或者
> slim-python 版本 Docker image 或者使用 full 版本的预编译二进制文件。若需要自行编译，请添加 `script` 编译参数。

## JavaScript 脚本函数

软件中内置了 JavaScript 解释器 [goja](https://github.com/dop251/goja)，可以直接使用 JavaScript 语言编写脚本函数。由于内置的解释器较大，默认编译的版本不包含脚本函数扩展功能，用户需要使用预编译的 full 版本或者 slim 版本 docker image。 自行编译时，需要添加 `script` build tag 。

用户使用 JavaScript 语言编写的脚本函数，一般的步骤如下：

1. 编写和调试 JavaScript 脚本函数
2. 将脚本函数注册到 eKuiper 中
3. 在 SQL 中使用脚本函数
4. 输入数据流，查看运行结果

### JavaScript 函数编写

用户可以使用喜爱的编辑器编写 JavaScript 脚本函数，并自行调试。脚本函数中需要包含函数定义和所有依赖的变量和其他函数。注册后，脚本函数将映射为一个 SQL 函数，使用相同的函数签名，并获得相同的返回值类型。

```javascript
function echo(msg) {
  return msg;
}
```

请注意，在函数中仅能使用 [goja](https://github.com/dop251/goja) 支持的语法和函数，即 ECMA 5.1 标准。由于 JavaScript 的弱类型特性与 SQL 的强类型特性不完全匹配，用户编写代码时应当自行考虑类型转换和验证。

#### 聚合函数

若函数需要作为聚合函数使用，则用户编写函数时应当预期收到的参数为数组，返回值为单个值。例如：

```javascript
function count_by_js(msgs) {
  return msg.length;
}
```

### JavaScript 函数管理

函数编写调试完成后，用户需要将函数注册到 eKuiper 中。注册的方式有两种：

1. 使用 [REST API](../../api/restapi/udf.md) 注册
2. 使用 [CLI](../../api/cli/scripts.md) 注册

注册时，需要提供函数的名称、函数代码文本等信息。注册成功后，即可在 SQL 中使用。同时，可通过 REST API 或 CLI 查看已注册的函数信息，以及更新或删除。

### 在 SQL 中使用

在目前版本中，注册完成的函数，可以在 SQL 中直接使用。但 SQL 层面不提供函数参数和返回值的静态校验。因此，用户需要自行保证函数的参数和返回值类型与 JavaScript 函数签名一致，或自行在函数实现中适配不同参数类型。用户可以在 JavaScript 函数中抛出异常。异常在运行规则中会作为运行时错误处理。

## 使用案例

假设用户已开发完成一个 JavaScript 用于计算面积脚本函数，可以使用如下步骤在规则中使用。

1. 注册函数

   ```http request
   POST udf/javascript

   {
     "id": "area",
     "description": "calculate area",
     "script": "function area(x, y) { log(\"Hello, World!\"); return x * y;}",
     "isAgg": false
   }
   ```

2. 在 SQL 中使用，假设已有 MQTT 数据流 `mqttDemo`，可创建如下规则：

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

3. 通过 MQTT 输入如下类似数据

   ```json
   { "length": 3, "width": 4 }
   ```

4. 订阅 MQTT 主题 `result/area`，查看持续输出的结果，例如

   ```json
   { "area": 21 }
   ```
