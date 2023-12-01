# 动态重载配置

通过动态重载[配置](../../configuration/global_configurations.md)，可以给运行中的 eKuiper 更新如 debug，timezone 之类的参数，而不用重启应用。

## 重载 Basic 配置

```shell
PATCH http://localhost:9081/configs
```

请求示例：

```json
{
  "debug": true,
  "consoleLog": true,
  "fileLog": true,
  "timezone": "UTC"
}
```

目前支持的动态可重载的参数有：

- `debug`
- `consoleLog`
- `fileLog`
- `timezone`

## 关闭 eKuiper

```shell
POST http://localhost:9081/stop
```

通过该 API 将 eKuiper 关机。
