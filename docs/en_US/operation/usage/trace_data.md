## Use Open Telemetry Tracing to track data

eKuiper's rule is a continuously running streaming computing task. Rules are used to process unbounded data flows. Under normal circumstances, rules will continue to run after they are started and continuously generate running status data. We can use open telemetry tracing to track the data changes of each piece of data in each operator.

## Open Telemetry Tracing configuration

You can expose Open Telemetry data to the remote Open Telemetry Collector through configuration. At the same time, eKuiper also supports built-in collection of Open Telemetry Trcing data. You can limit the maximum amount of saved data through the following configuration.

```yaml
openTelemetry:
  serviceName: kuiperd-service
  enableRemoteCollector: false
  remoteEndpoint: localhost:4318
  localTraceCapacity: 2048
```

## Enable rule-level tracing

You can turn on data link tracing for the corresponding rule by setting `enableRuleTracer` in the rule `options` to true. For specific settings, please see [Rules](../../guide/rules/overview.md#rules)

## Get the Trace ID of each piece of data

You can get the latest Trace ID corresponding to the rule through the Rest API.

[View the most recent Trace ID based on rule ID](../../api/restapi/trace.md#view-the-latest-trace-id-based-on-the-rule-id)

## View traced data flow changes based on Trace ID

If you configure an Open Telemetry Tracing collector, you can query the storage and query platform behind the Open Telemetry collector through the Trace ID. At the same time, you can also obtain detailed tracing data by accessing the local Rest API.

[View detailed tracing data based on Trace ID](../../api/restapi/trace.md#view-detailed-tracing-data-based-on-trace-id)
