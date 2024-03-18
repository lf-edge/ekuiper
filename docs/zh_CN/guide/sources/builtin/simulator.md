# 模拟器数据源

<span style="background:green;color:white;">stream source</span>
<span style="background:green;color:white;padding:1px;margin:2px">scan table source</span>

模拟器源提供了一种用于测试和演示目的的数据生成方式。它可以用来模拟来自设备或传感器的数据流。用户可以定义模拟数据的内容和发送间隔等。

## 配置

eKuiper
连接器可以通过[环境变量](../../../configuration/configuration.md#environment-variable-syntax)、[REST API](../../../api/restapi/configKey.md)
或配置文件进行配置，本节将介绍配置文件的使用方法。

模拟器源连接器的配置文件位于：`$ekuiper/etc/simulator.yaml`。该文件定义了模拟数据和生成数据的间隔。

```yaml
default:
  data:
    - temperature: 22.5
      humidity: 50
  interval: 10
  loop: true
```

用户可以指定以下属性：

- `data`: 要发送的模拟数据。其格式为 yaml 结构体。其中，键为发送数据的字段名，值为发送数据的值。该属性为结构体的列表。模拟器源会按顺序发送列表中的数据。
- `interval`: 数据发送的间隔时间，单位为毫秒。
- `loop`: 是否循环发送数据。如果设置为 true，连接器将依次发送列表中的数据，然后再从头开始。如果设置为
  false，连接器将在发送完列表中的所有数据后停止。

## 创建流数据源

完成连接器的配置后，后续可通过创建流将其与 eKuiper 规则集成。模拟器源连接器可以作为[流式](../../streams/overview.md)
或[扫描表数据源](../../tables/scan.md)使用，本节将以流类型源为例进行说明。

您可通过 REST API 或 CLI 工具在 eKuiper 中创建模拟器数据源。

### 通过 REST API 创建

REST API 为 eKuiper 提供了一种可编程的交互方式，适用于自动化或需要将 eKuiper 集成到其他系统中的场景。

**示例**

```sql
CREATE
STREAM mock_stream () WITH (TYPE="simulator");
```

详细操作步骤及命令解释，可参考 [通过 REST API 进行流管理](../../../api/restapi/streams.md)。

### 通过 CLI 创建

用户也可以通过命令行界面（CLI）直接访问 eKuiper。

1. 进入 eKuiper `bin` 目录：

   ```bash
   cd path_to_eKuiper_directory/bin
   ```

2. 使用 `create` 命令创建规则，指定 模拟器数据源，如：

   ```bash
   ./kuiper create stream mock_stream ' WITH (TYPE="simulator")'
   ```

详细操作步骤及命令解释，可参考 [通过 CLI 进行流管理](../../../api/cli/streams.md)。
