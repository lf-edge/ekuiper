wasm plugin 运行测试步骤：

1. 启动wasm_test_server.go服务器
2. 运行sdk/go/example/fib 文件夹中main.go
3. 向 http://localhost:33333/symbol/start 发送

```json
{
  "symbolName": "fib",
  "meta": {
    "ruleId": "rule1",
    "opId": "op1",
    "instanceId": 1
  },
  "pluginType": "func",
  "config": {}
}
```

注意点：wasmfile文件需要匹配本地目录，以防找不到。



待改进： 

1 参数多种类型转化

2 参数数目不同时的多种选择