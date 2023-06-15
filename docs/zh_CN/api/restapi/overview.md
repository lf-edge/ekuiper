除 CLI 外，eKuiper 还提供了一组用于流和规则管理的 REST API。

默认情况下，REST API 在端口9081中运行。您可以在 `/etc/kuiper.yaml` 中通过`restPort` 属性更改端口。

## 获取信息

该 API 用于获取版本号、系统类型、程序运行时长。

```shell
GET http://localhost:9081
```

```json
{
"version": "1.0.1-22-g119ee91",
"os": "darwin",
"upTimeSeconds": 14
}
```

## ping

```shell
GET http://localhost:9081/ping
```

- [流](streams.md)
- [规则](rules.md)
- [插件](plugins.md)
