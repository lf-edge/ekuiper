# Websocket 数据源

<span style="background:green;color:white;">stream source</span>

eKuiper 内置支持 Websocket 数据源，通过 Websocket 数据源连接器，eKuiper 可从外部 Websocket 客户端中获取数据。

## 服务器配置

服务器配置在 `etc/kuiper.yaml` 中的 `source` 部分。

```yaml
source:
  ## Configurations for the global websocket server for websocket source
  # HTTP data service ip
  httpServerIp: 0.0.0.0
  # HTTP data service port
  httpServerPort: 10081
  # httpServerTls:
  #    certfile: /var/https-server.crt
  #    keyfile: /var/https-server.key
```

用户可以指定以下属性：

- `httpServerIp`：用于绑定 Websocket 数据服务器的 IP。
- `httpServerPort`：用于绑定 Websocket 数据服务器的端口。
- `httpServerTls`：Websocket 服务器 TLS 的配置。

当任何需要 Websocket 源的规则被启动时，全局服务器的设置会初始化。所有关联的规则被关闭后，它就会终止。

## 创建流数据源

完成连接器的配置后，后续可通过创建流将其与 eKuiper 规则集成。Websocket 源连接器可以作为[流式](../../streams/overview.md) 使用，本节将以流类型源为例进行说明。

您可通过 REST API 或 CLI 工具在 eKuiper 中创建 Websocket 数据源。

### 通过 REST API 创建

REST API 为 eKuiper 提供了一种可编程的交互方式，适用于自动化或需要将 eKuiper 集成到其他系统中的场景。

**示例**

```sql
CREATE STREAM websocketDemo() WITH (FORMAT="json", TYPE="websocket")
```

**使用自定义配置**

您可以使用 `endpoint` 属性，该属性对应于流创建语句中的

创建流时，您可通过 `datasource` 配置项对应  `endpoint` 属性，如下所示：

**示例**

```sql
CREATE STREAM websocketDemo() WITH (DATASOURCE="/api/data", FORMAT="json", TYPE="websocket")
```

在以上示例中，我们将源绑定到 `/api/data` 端点。因此，它将监听 `http://localhost:10081/api/data`。

详细操作步骤及命令解释，可参考 [通过 REST API 进行流管理](../../../api/restapi/streams.md)。

### 通过 CLI 创建

用户也可以通过命令行界面（CLI）直接访问 eKuiper。

1. 进入 eKuiper `bin` 目录：

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. 使用 `create` 命令创建规则，指定 HTTP Push 数据源，如：

   ```bash
   bin/kuiper CREATE STREAM demo'() with(format="json", datasource="/api/data", type="websocket")'
   ```
详细操作步骤及命令解释，可参考 [通过 CLI 进行流管理](../../../api/cli/streams.md)。
