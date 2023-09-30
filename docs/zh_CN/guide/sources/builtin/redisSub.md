## RedisSub 数据源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white">scan table source</span>

eKuiper 内置支持 Redis 数据源，支持在 Redis 中进行数据查询,订阅频道。
注意，RedisSub 数据源可作为流和扫描表类数据源，而[Redis 源](./redis.md)可作为一个[查询表](../../tables/lookup.md)。

## 配置

在使用连接器连接 RedisSub 数据源之前，应先完成连接和其他相关参数配置。

RedisSub 源的配置文件位于 */etc/sources/redisSub.yaml*，您可以在其中指定 Redis 的连接信息等属性。

```yaml
default:
   address: 127.0.0.1:6379
   username: default
   db: 0
```

按照以上配置，eKuiper 将引用的 Redis 实例地址是127.0.0.1:6379。值的类型是 "string"。

**配置项**

- **`address`**：指定 Redis 服务器的地址，格式为 `hostname:port` 或 `IP_address:port` 的字符串。
- **`username`**：设置用于访问 Redis 服务器的用户名，只有在服务器启用身份验证时需要配置。
- **`password`**：设置用于访问 Redis 服务器的密码，只有在服务器启用身份验证时需要配置。
- **`db`**：选择要连接的 Redis 数据库。默认是 0。
- **`channels`**：用于指定要订阅的 Redis 频道列表。
- **`decompression`**：指定用于解压缩 Redis Payload 的压缩方法，支持的压缩方法有"zlib","gzip","flate",zstd"。

## 创建流数据源

完成连接器的配置后，后续可通过创建流将其与 eKuiper 规则集成。RedisSub 源连接器可以作为[流式](../../streams/overview.md)或[扫描表数据源](../../tables/scan.md)使用，本节将以流类型源为例进行说明。

您可通过 REST API 或 CLI 工具在 eKuiper 中创建 RedisSub 数据源。

### 通过 REST API 创建

REST API 为 eKuiper 提供了一种可编程的交互方式，适用于自动化或需要将 eKuiper 集成到其他系统中的场景。

**示例**

```sql
CREATE STREAM redis_stream () WITH (FORMAT="json", TYPE="redisSub");
```

详细操作步骤及命令解释，可参考 [通过 REST API 进行流管理](../../../api/restapi/streams.md)。

### 通过 CLI 创建

用户也可以通过命令行界面（CLI）直接访问 eKuiper。

1. 进入 eKuiper `bin` 目录：

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. 使用 `create` 命令创建规则，指定 Redis Sub 数据源，如：

   ```bash
   ./kuiper create stream neuron_stream ' WITH (FORMAT="json", TYPE="redisSub")'
   ```

详细操作步骤及命令解释，可参考 [通过 CLI 进行流管理](../../../api/cli/streams.md)。
