# 连接管理

Source 和 Sink 用于与外部系统的交互，其中都会包含连接外部资源的动作。本章主要讲解 eKuiper 中对连接的管理。

## 连接类型

不同的连接类型复杂度各不相同，例如 MQTT 长连接需要关注连接的状态，规则运行中可能出现连接的断连，需要自动重连等复杂的管理；而
HTTP 连接默认为无状态的连接，状态管理较为简单。为了统一管理复杂的连接资源创建，复用，自动重连以及获取连接状态等功能，eKuiper
v2 增加了内部的连接池组件，并适配了一系列连接类型：

- MQTT 连接
- Neuron 连接
- EdgeX 连接
- SQL 连接
- HTTP 连接 （包括 REST sink，HTTP Pull source，HTTP push source 使用的连接）
- WebSocket 连接

其余连接类型可能会在后续版本中陆续接入。接入连接池的连接类型可通过 API 进行资源的独立创建，并获取 API。

eKuiper 中对各种连接的生命周期的管理分为 3 种：

1. 连接附属于规则：默认情况下，连接由 Source/Sink 的实现自行管理，其生命周期由使用的规则进行控制。规则启动时，使用到的连接资源才会开始连接；规则结束时，连接将被关闭。在下例中，我们创建了
   memory 类型的数据流 memStream。由于该类型未接入连接池，只有当使用该流的规则启动时，才会进行连接。

   ```sql
   create stream memStream () WITH (TYPE="mqtt", DATASOURCE="demo")
   ```

2. 连接池管理的匿名连接资源：部分连接类型适配了连接池管理接口，其生命周期由连接池管理。当启动包含这些类型连接的规则时，规则会从连接池获取匿名（实际资源
   id 由规则生成，且不会被共享）资源。在下例中，我们创建了 mqtt 类型的数据流 mqttStream。连接为匿名连接，由于该类型适配了连接池，我们可以在连接
   API 中获取到该连接。规则删除时，对应连接也会删除。

   ```sql
   create stream mqttStream () WITH (TYPE="mqtt", DATASOURCE="demo")
   ```

3. 用户创建的连接资源：用户可通过[连接管理API](../../api/restapi/connection.md) 进行资源的增删改查。通过 API 创建的资源必须指定唯一的
   id，规则中可引用此处创建的规则资源。**请注意**：只有适配连接池的连接资源可通过 API
   进行管理。这种连接类型创建的连接为独立的物理连接，创建完后会立即运行，无需依附于规则。它可以被多个规则，或者多个
   source/sink 共用。

**请注意**：用户创建的连接为实体连接，会自动重连直到连接成功为止。

### 连接重用

用户创建的连接资源可以独立运行，多个规则可以引用该命名资源。连接重用是通过 `connectionSelector`
配置项进行配置。用户只需创建一次连接资源即可复用，提升连接管理效率，简化配置流程。

1. 创建资源，如下例通过 API 创建 id 为 mqttcon1 的连接。可在 props 中配置连接需要的参数。连接创建成功后，mqttcon1 可在连接列表
   API 中找到。

   ```shell
   POST http://localhost:9081/connections
   {
     "id": "mqttcon1"
     "typ":"mqtt",
     "props": {
       server: "tcp://127.0.0.1:1883"
     }
   }
   ```

2. 在数据源中使用。在配置 MQTT 源（`$ekuiper/etc/mqtt_source.yaml`）时，可通过 `connectionSelector`
   引用以上连接配置，例如`demo_conf` 和 `demo2_conf` 都将引用 `mqttcon1` 的连接配置。

```yaml
#Override the global configurations
demo_conf: #Conf_key
  qos: 0
  connectionSelector: mqttcon1
  servers: [tcp://10.211.55.6:1883, tcp://127.0.0.1]

#Override the global configurations
demo2_conf: #Conf_key
  qos: 0
  connentionSelector: mqttcon1
  servers: [tcp://10.211.55.6:1883, tcp://127.0.0.1]
```

基于 `demo_conf` 和 `demo2_conf` 分别创建两个数据流 `demo` 和 `demo2`：

```text
demo (
    ...
  ) WITH (DATASOURCE="test/", FORMAT="JSON", CONF_KEY="demo_conf");

demo2 (
    ...
  ) WITH (DATASOURCE="test2/", FORMAT="JSON", CONF_KEY="demo2_conf");

```

当相应的规则分别引用以上数据流时，规则之间的源部分将共享连接。在这里 `DATASOURCE` 对应 mqtt 订阅的 topic，配置项中的 `qos`
将用作订阅时的 `Qos`。在以上示例配置中，`demo` 以 Qos 0 订阅 topic `test/`，`demo2` 以 Qos 0 订阅 topic `test2/` 。

::: tip

对于MQTT源，如果两个流具有相同的 `DATASOURCE` 但 `qos` 值不同，则只有先启动的规则才会触发订阅。

:::

也可以在规则的 action 中，通过 connentionSelector 重用定义的连接资源。

## 连接状态

连接状态分成 3 种：

1. 已连接，指标中用 1 表示。
2. 连接中，指标中用 0 表示。
3. 未连接，指标中用 1 表示。

用户可通过连接 API 获取连接的状态。同时，用户也可通过规则的指标查看规则 source/sink
中连接的状态，例如 `source_demo_0_connection_status` 指标表示 demo
流的连接状态。所有支持的连接指标请查看[指标列表](../../operation/usage/monitor_with_prometheus.md#运行指标)。
