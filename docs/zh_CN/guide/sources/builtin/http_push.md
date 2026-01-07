# HTTP Push 数据源

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

eKuiper 提供了内置的 HTTP 源。此时，eKuiper 将作为一个 HTTP 服务器来接收 HTTP 客户端的消息。所有的 HTTP 推送源共用单一的全局 HTTP 数据服务器。每个源可以有自己的 URL，这样就可以支持多个端点。

eKuiper 连接器可以通过[环境变量](../../../configuration/configuration.md#environment-variable-syntax)、[REST API](../../../api/restapi/configKey.md) 或配置文件进行配置，本节将介绍配置文件的使用方法。

## 服务器配置

服务器配置在 `etc/kuiper.yaml` 中的 `source` 部分。

```yaml
source:
  ## Configurations for the global http data server for httppush source
  # HTTP data service ip
  httpServerIp: 0.0.0.0
  # HTTP data service port
  httpServerPort: 10081
  # httpServerTls:
  #    certfile: /var/https-server.crt
  #    keyfile: /var/https-server.key
```

用户可以指定以下属性：

- `httpServerIp`：用于绑定 HTTP 数据服务器的 IP。
- `httpServerPort`：用于绑定 HTTP 数据服务器的端口。
- `httpServerTls`：HTTP 服务器 TLS 的配置。

当任何需要 HTTP Push 源的规则被启动时，全局服务器的设置会初始化。所有关联的规则被关闭后，它就会终止。

## 数据源配置

HTTP Push 源配置文件位于 `etc/sources/httppush.yaml`，其中：

- default：对应全局连接配置。
- 自定义部分：对于需要自定义某些连接参数的场景，该部分的配置将覆盖全局连接配置。

以下示例包括一个全局配置和自定义配置 `application_conf`：

```yaml
#Global httppush configurations
default:
  # the request method to listen on
  method: "POST"

#Override the global configurations
application_conf: #Conf_key
  server: "PUT"
```

::: tip

注意：目前只有 `method` 属性可用于配置要监听的 HTTP 方法。

:::

此外，每个[流](../../streams/overview.md)可以配置自己的 URL 端点和 HTTP 请求方法。端点属性被映射到创建流语句中的 `datasource` 属性。

## 创建流数据源

完成连接器的配置后，后续可通过创建流将其与 eKuiper 规则集成。HTTP Push 源连接器可以作为[流式](../../streams/overview.md)或[扫描表类数据源](../../tables/scan.md)使用，本节将以流类型源为例进行说明。

您可通过 REST API 或 CLI 工具在 eKuiper 中创建 HTTP Push 数据源。

### 通过 REST API 创建

REST API 为 eKuiper 提供了一种可编程的交互方式，适用于自动化或需要将 eKuiper 集成到其他系统中的场景。

**示例**

```sql
CREATE STREAM httpDemo() WITH (FORMAT="json", TYPE="httppush")
```

**使用自定义配置**

您可以使用 `endpoint` 属性，该属性对应于流创建语句中的

创建流时，您可通过 `datasource` 配置项对应 `endpoint` 属性，如下所示：

**示例**

```sql
CREATE STREAM httpDemo() WITH (DATASOURCE="/api/data", FORMAT="json", TYPE="httppush")
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
   bin/kuiper CREATE STREAM demo'() with(format="json", datasource="/api/data type="httppush")'
   ```

详细操作步骤及命令解释，可参考 [通过 CLI 进行流管理](../../../api/cli/streams.md)。
