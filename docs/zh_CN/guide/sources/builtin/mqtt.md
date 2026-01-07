# MQTT 数据源

<span style="background:green;color:white;padding:1px;margin:2px">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

MQTT（Message Queuing Telemetry Transport）是一种轻量级的通信协议，用于在物联网设备之间进行可靠的消息传递。eKuiper 内置 MQTT 连接器，方便订阅来自 MQTT 代理的消息并输入 eKuiper 处理管道，实现对指定 MQTT 主题的实时数据处理。

在 eKuiper 中，MQTT 连接器可以作为源连接器（从 MQTT 代理获取数据）或 [Sink 连接器](../../sinks/builtin/mqtt.md)（将数据发布到 MQTT 代理），本节重点介绍 MQTT 源连接器。

## 配置

eKuiper 连接器可以通过[环境变量](../../../configuration/configuration.md#environment-variable-syntax)、[REST API](../../../api/restapi/configKey.md) 或配置文件进行配置，本节将介绍配置文件的使用方法。

MQTT 源连接器的配置文件位于：`$ekuiper/etc/mqtt_source.yaml`，其中：

- default：对应全局连接配置。
- 自定义部分：适用于需要自定义连接参数的场景，该部分的配置将覆盖全局连接配置。
- 连接器重用：eKuiper 还支持通过 [`connectionSelector`](../../connector.md#connection-selector) 配置项在不同的配置中复用某个连接配置。

以下示例包括一个全局配置和自定义配置 `demo_conf`：

```yaml
#全局 MQTT 配置
default:
  qos: 1
  server: "tcp://127.0.0.1:1883"
  #username: user1
  #password: password
  #certificationPath: /var/kuiper/xyz-certificate.pem
  #privateKeyPath: /var/kuiper/xyz-private.pem.key
  #rootCaPath: /var/kuiper/xyz-rootca.pem
  #insecureSkipVerify: true
  #connectionSelector: mqtt.mqtt_conf1
  # 使用指定的压缩方法解压缩。现在支持`gzip`、`zstd`
  # decompression: ""

#覆盖全局配置
demo_conf: #Conf_key
  qos: 0
  server: "tcp://10.211.55.6:1883"
```

你可以通过 api 的方式提前检查对应 sink 端点的连通性: [连通性检查](../../../api/restapi/connection.md#连通性检查)

## 全局配置

用户可在 `default` 部分指定全局设置。

### 连接相关配置

- `qos`：默认订阅 QoS 级别。
- `server`：MQTT 服务器。
- `username`：MQTT 连接用户名。
- `password`：MQTT 连接密码。
- `protocolVersion`：MQTT 协议版本。可选值：3.1 (MQTT 3) 或 3.1.1 (也被称为 MQTT 4)。如未指定，则将使用缺省值：3.1。
- `clientid`：MQTT 连接的客户端 ID。如未指定，将使用 uuid。

### 安全和认证配置

- `certificationPath`: 证书路径，示例值：`d3807d9fa5-certificate.pem`。可以是绝对路径，也可以是相对路径。如指定相对路径，那么父目录为执行 `kuiperd` 命令的路径，例如：
  - 如果在 `/var/kuiper` 中运行 `bin/kuiperd` ，那么父目录为 `/var/kuiper`。
  - 如果运行从`/var/kuiper/bin`中运行`./kuiperd`，那么父目录为 `/var/kuiper/bin`。
- `privateKeyPath`：私钥路径，示例值：`d3807d9fa5-private.pem.key`。可以是绝对路径，也可以是相对路径，具体可参考 `certificationPath`。
- `rootCaPath`：根证书路径。可以是绝对路径，也可以是相对路径。
- `certificationRaw`: 经过 base64 编码过的证书原文, 如果同时定义了 `certificationPath` 将会先用该参数。
- `privateKeyRaw`: 经过 base64 编码过的密钥原文， 如果同时定义了 `privateKeyPath` 将会先用该参数。
- `rootCARaw`: 经过 base64 编码过的根证书原文， 如果同时定义了 `rootCaPath` 将会先用该参数。
- `insecureSkipVerify`：是否跳过证书验证。如设置为 `true`，TLS 接受服务器提供的任何证书以及该证书中的任何主机名。注意：此时，TLS 容易受到中间人攻击。默认值：`false`。

### 连接重用

- `connectionSelector`: 重用 MQTT 数据源连接，如下方配置示例中的 `mqtt.localConnection`。注意：连接配置文件位于 `connections/connection.yaml`。有关连接重用的详细解释，见[连接器的重用](../../connector.md#连接器的重用)。

  ```yaml
  #全局 MQTT 连接
  default:
    qos: 1
    server: "tcp://127.0.0.1:1883"
    #username: user1
    #password: password
    #certificationPath: /var/kuiper/xyz-certificate.pem
    #privateKeyPath: /var/kuiper/xyz-private.pem.key
    connectionSelector: mqtt.localConnection
  ```

  ::: tip

  指定 `connectionSelector` 参数后，所有关于连接的参数都会被忽略，例如上例中的 `server:"tcp://127.0.0.1:1883"`。

  :::

### **负载相关配置**

- `decompression`：使用指定的压缩方法解压缩，支持 `gzip`、`zstd`。
- `bufferLength`：指定最大缓存消息数目。该参数主要用于防止内存溢出。实际内存用量会根据当前缓存消息数目动态变化。增大该参数不会增加初始内存分配量，因此建议设为较大的数值。默认值为102400；如果每条消息为100字节，则默认情况下，缓存最大占用内存量为102400 \* 100B ~= 10MB.

### **KubeEdge 集成**

- `kubeedgeVersion`：KubeEdge 版本号，不同的版本号对应的文件内容不同。

- `kubeedgeModelFile`：KubeEdge 模版文件名，文件路径为：`etc/sources`，样例格式如下：

  ```yaml
  {
    "deviceModels":
      [
        {
          "name": "device1",
          "properties":
            [
              { "name": "temperature", "dataType": "int" },
              { "name": "temperature-enable", "dataType": "string" },
            ],
        },
      ],
  }
  ```

  其中，
  - `deviceModels.name`：设备名称，与订阅主题中的字段匹配，为第三和第四个“/”之间的内容。例如：$ke/events/device/device1/data/update，设备名称为 `device1`。
  - `properties.name`：字段名称。
  - `properties.dataType`：预期字段类型。

## 自定义配置

对于需要自定义某些连接参数的场景，eKuiper 支持用户创建自定义模块来实现全局配置的重载。

**配置示例**

```yaml
#覆盖全局配置
demo_conf: #Conf_key
  qos: 0
  server: "tcp://10.211.55.6:1883"
```

定义 `demo_conf` 配置组后，如希望在创建流时使用此配置，可通过 `CONF_KEY` 选项并指定配置名称，此时，在自定义配置中定义的参数将覆盖 `default` 配置中的相应参数。详细步骤，可参考 [流语句](../../../sqls/streams.md)。

**示例**

```sql
demo (
    ...
  ) WITH (DATASOURCE="test/", FORMAT="JSON", KEY="USERID", CONF_KEY="demo_conf");
```

## 创建流类型源

完成连接器的配置后，后续可通过创建流将其与 eKuiper 规则集成。MQTT 源连接器可以作为[流式](../../streams/overview.md)或[扫描表数据源](../../tables/scan.md)使用，本节将以流类型源为例进行说明。

您可通过 REST API 或 CLI 工具在 eKuiper 中创建 MQTT 数据源。

### 通过 REST API 创建

REST API 为 eKuiper 提供了一种可编程的交互方式，适用于自动化或需要将 eKuiper 集成到其他系统中的场景。

**示例**

```sql
{"sql":"create stream my_stream (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\")"}
```

详细操作步骤及命令解释，可参考 [通过 REST API 进行流管理](../../../api/restapi/streams.md)。

### 通过 CLI 创建

用户也可以通过命令行界面（CLI）直接访问 eKuiper。

1. 进入 eKuiper `bin` 目录：

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. 使用 `create` 命令创建规则，指定 MQTT 数据源，如：

   ```bash
   bin/kuiper create stream my_stream '(id bigint, name string, score float) WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id")'
   ```

详细操作步骤及命令解释，可参考 [通过 CLI 进行流管理](../../../api/cli/streams.md)。

## 迁移指南

从 eKuiper 1.5.0 开始，eKuiper 将 MQTT 源地址配置从 `servers` 更改为 `server`，即用户只能配置一个 MQTT 源地址而不是一个地址数组。对需要进行版本升级的用户：

- 如希望通过配置文件配置，请确保 `etc/mqtt_source.yaml` 文件内的 `server` 已正确配置。
- 如希望通过环境变量配置，例如针对 `tcp://broker.emqx.io:1883` 地址，配置命令应从 `MQTT_SOURCE__DEFAULT__SERVERS=[tcp://broker.emqx.io:1883]` 改为 `MQTT_SOURCE__DEFAULT__SERVER="tcp://broker.emqx.io:1883"`。
