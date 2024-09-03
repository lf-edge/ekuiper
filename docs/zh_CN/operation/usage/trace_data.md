## 使用 Open Telemetry Tracing 来追踪数据

eKuiper 的规则是一个持续运行的流式计算任务。规则用于处理无界的数据流，正常情况下，规则启动后会一直运行，不断产生运行状态数据。我们可以使用 open telemetry tracing 来追踪每一条数据在各个算子中的数据变化。

## Open Telemetry Tracing 配置

你可以通过配置将 Open Telemetry 的数据暴露给远端的 Open Telemetry Collector, 同时 eKuiper 也支持内置收集 Open Telemtry Trcing 的数据，你可以通过以下配置限制最大保存数据的数量。

```yaml
openTelemetry:
  enableRemoteCollector: false
  remoteEndpoint: localhost:4318
  localTraceCapacity: 2048
```

## 开启规则级别的追踪

你可以通过设置规则 `options` 中的 `enableRuleTracer` 为 true，为对应规则打开数据链路追踪。具体设置请查看 [规则](../../guide/rules/overview.md#选项)

## 获取每条数据的 Trace ID

你可以通过 Rest API 获取规则对应的最近 Trace ID。

[根据规则 ID 查看最近的 Trace ID](../../api/restapi/trace.md#根据规则-id-查看最近的-trace-id)

## 根据 Trace ID 查看追踪的数据流变化

如果你配置了 Open Telemetry Tracing 收集器，你可以通过 Trace ID 向 Open Telemetry 收集器背后的存储与查询平台进行查询。 同时，你也可以通过访问本地 Rest API 的方式获取详细的追踪数据。

[根据 Trace ID 查看详细追踪数据](../../api/restapi/trace.md#根据-trace-id-查看详细追踪数据)