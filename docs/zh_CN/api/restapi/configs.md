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

## 重置 eKuiper

```shell
POST http://localhost:9081/reset
```

> [!WARNING]
> `/reset` 是一项破坏性的系统管理操作，会永久清空运行时的持久化状态。在生产环境中请确保开启 [REST API 安全认证](authentication.md)，以防止未授权的重置请求。

查询参数：

- `force`（布尔值，可选，默认为 `false`）：在正常运行模式下必须显式指定 `force=true` 以防止误操作；在降级模式下无需携带该参数。

该 API 用于清空 eKuiper 运行时持久化数据并触发服务重启，以执行出厂重新初始化：

- **运行时持久化数据清理**：清空 `data` 目录下的所有运行时数据文件与子目录（包括 SQLite 数据库 `sqliteKV.db`、`cache.db`、`checkpoint.db`、`extState.db`、`trace.db` 等），但会保留 `data/init.json`（以及 `etc/init.json`）等出厂初始配置文件。
- **初始化标记重置**：删除 `etc` 目录下的 `initialized*` 标记文件，使系统在重启后能重新导入并应用 `init.json` 规则集。
- **存储后端限制**：目前支持 SQLite 存储后端（`store.type: sqlite`）。若在非 SQLite 存储后端下调用，将返回 HTTP 400 错误。

运行模式差异：

- **降级模式**：允许直接调用重置，无需附加参数（`POST http://localhost:9081/reset`）。
- **正常模式**：必须携带 `?force=true`（`POST http://localhost:9081/reset?force=true`）。未指定 `force=true` 时返回 HTTP 400 明确拒绝。重置请求被接受后，系统会拒绝新的业务请求，安全停止正在运行的规则与后台组件，关闭存储连接，完成数据清理后退出进程，由容器或守护进程自动重启。
- **防重复触发**：若重置已在进行中，后续重置请求将返回 HTTP 409 Conflict。

成功返回（HTTP 202 Accepted）：

```json
{
  "message": "reset process initiated, eKuiper is resetting and restarting..."
}
```

eKuiper 进程在发出该响应后，执行有序关闭和运行时数据清理并退出。由外部进程管理器（如 systemd 或容器守护进程）重新拉起，基于初始配置恢复出厂状态。
