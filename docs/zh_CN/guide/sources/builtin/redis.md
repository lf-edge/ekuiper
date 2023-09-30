## Redis 数据源

<span style="background:green;color:white">lookup table source</span>

eKuiper 内置支持 Redis 数据源，支持在 Redis 中进行数据查询。
注意，现在 Redis 源只能作为一个[查询表](../../tables/lookup.md)，而[RedisSub 数据源](./redisSub.md)可作为流和扫描表类数据源。

## 配置

在使用连接器连接 Redis 数据源之前，应先完成连接和其他相关参数配置。

Redis 源的配置文件位于 */etc/sources/redis.yaml*，您可以在其中指定 Redis 的连接信息等属性。

```yaml
default:
  # the redis host address
  addr: "127.0.0.1:6379"
  # currently supports string and list only
  datatype: "string"
#  username: ""
#  password: ""
```

按照以上配置，eKuiper 将引用的 Redis 实例地址是127.0.0.1:6379。值的类型是 "string"。

**配置项**

- **`addr`**：指定 Redis 服务器的地址，格式为 `hostname:port` 或 `IP_address:port` 的字符串。
- **`datatype`**：确定连接器应从 Redis 键中预期的数据类型。目前仅支持 `string` 和 `list`。
- **`username`**：设置用于访问 Redis 服务器的用户名，只有在服务器启用身份验证时需要配置。
- **`password`**：设置用于访问 Redis 服务器的密码，只有在服务器启用身份验证时需要配置。

## 创建查询表数据源

完成连接器的配置后，后续可通过创建流将其与 eKuiper 规则集成。我们可以定义一个流指定 Redis 的源、配置及数据格式。

您可通过 REST API 或 CLI 工具进行配置。

### 通过 REST API 创建

REST API 为 eKuiper 提供了一种可编程的交互方式，适用于自动化或需要将 eKuiper 集成到其他系统中的场景。

**示例**

```sql
create table table1 () WITH (DATASOURCE="0", FORMAT="json", TYPE="redis", KIND="lookup");
```

详细操作步骤及命令解释，可参考 [通过 REST API 进行流管理](../../../api/restapi/streams.md)。

### 通过 CLI 创建

用户也可以通过命令行界面（CLI）直接访问 eKuiper。

1. 进入 eKuiper `bin` 目录：

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. 使用 `create` 命令创建规则，指定 Redis 数据源，如：

   ```bash
   ./kuiper create stream neuron_stream ' WITH (DATASOURCE="0", FORMAT="json", TYPE="redis", KIND="lookup")'
   ```

详细操作步骤及命令解释，可参考 [通过 CLI 进行流管理](../../../api/cli/streams.md)。
