除 CLI 外，eKuiper 还提供了一组用于流和规则管理的 REST API。

默认情况下，REST API 在端口9081中运行。您可以在 `/etc/kuiper.yaml` 中通过`restPort` 属性更改端口。

## 获取信息

该 API 用于获取版本号、系统类型、程序运行时长以及服务运行状态。

```shell
GET http://localhost:9081
```

正常运行时的返回示例：

```json
{
  "version": "2.1.0",
  "os": "linux",
  "upTimeSeconds": 14,
  "status": "running",
  "degraded": false
}
```

主库故障进入降级模式时的返回示例：

```json
{
  "version": "2.1.0",
  "os": "linux",
  "upTimeSeconds": 14,
  "status": "degraded",
  "degraded": true,
  "degradedReason": "file is not a database (26)",
  "reason": "file is not a database (26)"
}
```

## ping

```shell
GET http://localhost:9081/ping
```

## 降级模式与存储故障处理

在启动阶段，eKuiper 会通过 `PRAGMA quick_check(1)` 对底层 SQLite 数据库执行快速完整性校验。

- **主数据库故障**：若主数据库（`sqliteKV.db`）初始化失败（如数据损坏或 I/O 错误），eKuiper 不会自动删除或丢弃现有数据，而是进入**降级模式**保活，保持 REST 管理接口可用，同时在磁盘上完整保留数据库文件，等待管理员进行排查、诊断或恢复。
- **辅助数据库故障**：若辅助数据库（如 `checkpoint.db`、`cache.db`、`extState.db`、`trace.db` 等）初始化失败，eKuiper 采取 **Best-effort（尽力而为）** 策略：主服务继续以正常模式启动（`status: "running"`），保留磁盘文件不自动删除。不依赖该辅助库的功能与规则继续正常工作，依赖故障辅助库的功能安全报错。

在降级模式下：
- 跳过流与规则处理器加载、后台组件初始化及规则恢复，防止系统异常崩溃。
- 仅放行维护与健康检查端点：`GET /`、`GET /ping`、`POST /reset` 以及 `POST /stop`（或 `GET /stop`）。
- 访问其他业务端点时统一返回 HTTP `503 Service Unavailable` 提示服务处于降级模式，建议调用 `POST /reset`。
- 管理员可在排查修复底层数据库后直接重启系统，或调用 `POST /reset` 清理运行时持久化状态并重新初始化。

## 批量请求

该 API 用于将多个请求合并为一个请求发送执行

```shell
POST http://localhost:9081/batch/req

[
    {
        "method": "POST",
        "path": "/streams",
        "body": "{\"sql\":\"CREATE stream demobatch() WITH (DATASOURCE=\\\"/data1\\\", TYPE=\\\"websocket\\\")\"}"
    },
    {
        "method": "GET",
        "path": "/streams/demobatch"
    }
]

Response

[
    {
        "code": 201,
        "response": "Stream demobatch is created."
    },
    {
        "code": 200,
        "response": "{\"Name\":\"demobatch\",\"StreamFields\":null,\"Options\":{\"datasource\":\"/data1\",\"type\":\"websocket\"},\"StreamType\":0,\"Statement\":null}"
    }
]
```

- [流](streams.md)
- [规则](rules.md)
- [插件](plugins.md)
- [配置与运维](configs.md)
