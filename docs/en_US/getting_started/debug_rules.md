# How to Debug Rules

eKuiper is a lightweight and high-performance SQL engine for edge computing. It allows you to write SQL-like rules to
process streaming data from various sources and send the results to different sinks. Sounds cool, right?

But what if your rules don't work as expected? How do you find out what's wrong and fix it? Don't worry, I've got you
covered. Here are some steps you can follow to debug eKuiper rules like a pro.

## Create the Rule

To debug a rule, the first step is to create it. You can do this by using the REST API or CLI. In this tutorial, we'll
use the REST API for all rule management actions. Below is an example to create a rule using the REST API:

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT values.tag1 AS temperature, values.tag2 AS humidity FROM neuronStream",
  "actions": [
    {
      "influx": {
        "addr": "http://10.11.71.70:8086",
        "username": "",
        "password": "",
        "measurement": "test",
        "databasename": "mydb",
        "tagkey": "tagkey",
        "tagvalue": "tagvalue",
        "fields": "humidity,temperature"
      }
    }
  ]
}
```

### Debugging Tips

If there are problems when creating a rule, you will always get an error message. So debugging rule creation is pretty
straightforward: just check if you got an error message and what it says.

#### Check your http response

In the tool you are using to send the http request, you should see the response from the server. If rule creation is
successful, you will get a response like this:

```json
{
  "code": 200,
  "message": "OK"
}
```

If there is an error, it will be displayed in the response body. For example, if you try to create a rule with an
invalid SQL statement, you will get an error message like this:

```json
{
  "code": 400,
  "message": "invalid sql: near \"SELEC\": syntax error"
}
```

#### Check the logs

The error message in the response body is usually enough to tell you what's wrong. But if you want to know more details
about the error, you can check the logs of the eKuiper server.

The logs are located in the `logs` directory under the eKuiper installation directory. You can use the `tail` command to
view the logs in real time.

If you are using the Docker image, make sure to enable console log by environment
variable `KUIPER__BASIC__CONSOLELOG=true` or edit `etc/kupier.yaml` and set `consoleLog` to true.

Then you can use the `docker logs` command to view the logs and keep an eye on the error log.

### Common Errors

When submitting a rule, eKuiper will validate the rule and run it. You may encounter some errors. Here are some common
errors:

#### Syntax error

**1. SQL syntax error**

For example, to submit a rule with SQL `SELECT temperature humidity FROM sensor`, you will get an error message like
this:

```text
HTTP/1.1 400 Bad Request

invalid rule json: Parse SQL SELECT temperature humidity FROM neuronStream error: found "humidity", expected FROM..
```

Missing comma between two fields, thus the SQL parser thinks `humidity` is a table name and expected from before it.

To fix errors like "Parse SQL xxx error", just review the SQL syntax and correct it.

**2. Stream isn't found**

In eKuiper, you need to create a stream before you can use it in a rule. If you try to use a stream that doesn't exist,
you will get an error message like this:

```text
HTTP/1.1 400 Bad Request

create rule topo error: fail to get stream myStream, please check if stream is created
```

To fix this error, you need to create the stream first. You can use the REST API to check the current streams and create
a new stream if necessary.

**3. Rule ID exists**

Rule ID is unique in eKuiper. If you try to create a rule with an ID that already exists, you will get an error message
like this:

```text
HTTP/1.1 400 Bad Request

store the rule error: Item rule1 already exists
```

To fix this, you need to use a different ID for your rule or delete the existing rule first.

## Diagnose the Rule

If your rule is created successfully, it will be run immediately by default. If your rule is expected to send the result
to a MQTT topic, you may have subscribed that topic and wait to check the result. But if your rule doesn't work as
expected, you may want to diagnose it to find out what's wrong.

### Debugging Tips

You can follow these steps to diagnose your rule:

**1. Check rule status**

In rule creation, we only do some static validation for the syntax. When coming to run the rule, there are more things
to consider, such as the data source may not be available at runtime. So the first step is to check the rule status to
see if it is running or is stopped due to some runtime errors.

You can use the REST API to check the rule status. For example, to check the status of rule `rule1`, you can send a
request like this:

```http request
###
GET http://{{host}}/rules/rule1/status
```

If the rule is not running well, you will get a response like this:

```json
{
  "status": "stopped",
  "message": "Stopped: mqtt sink is missing property topic."
}
```

The message tells you the reason why the rule is stopped.

**2. Check the metrics**

If the rule is running well, but you still not get the result you expected, you can check the metrics to see if there is
any problem.

Use the status API in the previous section to get the rule metrics. The metrics include all nodes from source,
processors to sinks, in the rule. Each node has the status like message read in, writes out, latency, etc.

Firstly, take a look at the source metrics like below. If your source `records_in_total` is 0, it means that the source
is not receiving any data. You need to check the source side: if the data source has emitted data; if your source
configuration is correct. For example, if your MQTT source topic is configured to `topic1`, but you send data
to `topic2`, then the source will not receive any data which can be observed by the source metric.

```text
"source_demo_0_records_in_total": 0,
"source_demo_0_records_out_total": 0,
```

If the source metrics are good, then you can check the metrics of the processors and then the sinks. For example, if you
have `WHERE` clause, the rule pipeline will have a `filter` processor. Filter processor will filter out data before
sending it out to sink, thus you will find nothing received in the sink. You can check the `filter_xxx_records_in_total`
and `filter_xxx_records_out_total` metric. If `records_out` and `records_in` is not the same, it means some data are
filtered. It the  `records_out` is 0, it means that all data are filtered out. If that's not expected, you need to check
the real data. This needs to open the debug log and check OR create debug rules with the data printed out. We will cover
this in the next section.

**3. Check the debug logs**

If the status is stopped, you can check the logs to check the detail. If the status is running and the metrics are not
as expected, you can check the logs to see if there is any error or even open debug to track the data flow.

Here is [the instruction to check the logs](#check-the-logs). To open debug log, you can set the log level to `debug` in
the `etc/kuiper.yaml` file or setting environment variable: `KUIPER__BASIC__DEBUG=false`. Then you can check the debug
log to see the data flow. For example, the below is one line of debug log regarding filter.

```text
time="2023-05-31 14:58:43" level=debug msg="filter plan receive &{mockStream map[temperature:%!s(float64=-11.77) ts:%!s(float64=1.684738889251e+12)] %!s(int64=1685516298342) map[fi
le:C:\\repos\\go\\src\\github.com\\lfedge\\ekuiper\\data\\mock.lines] {{{%!s(int32=0) %!s(uint32=0)} %!s(uint32=0) %!s(uint32=0) {{} %!s(int32=0)} {{} %!s(int32=0)}} map[] map[]} {%!s(int32=0) %!s(uint32=0)} map[]}" file="operator/filter_operator.go:36" rule=rule1
```

The last of the line has `rule=rule1` which means this line of log is printed by rule1. Among the log, you can find the
data received by filter plan is
like `mockStream map[temperature:%!s(float64=-11.77) ts:%!s(float64=1.684738889251e+12)]`. This means the stream name is
mockStream, the payload is a map with `temperature=-11.77 and ts=1.684738889251e+12`. Then check your `WHERE` condition
against the data to see if it runs well.

**4. Create debug rules**

Reading the debug log may be overwhelming. Alternatively, you can create a debug rule to print out the data. For
example, if your rule in production sends data to MQTT, you can add a `log` sink to also print the result in the log.

```json
{
  "id": "rule1",
  "sql": "SELECT * FROM mockStream WHERE temperature > 30",
  "actions": [
    {
      "mqtt": {
        "server": "{{broker address}}",
        "topic": "topic1"
      },
      "log": {
      }
    }
  ]
}
```

Another example is diagnosing the filter. You can create another rule to print out all the data received to see if the
filter works as expected.

```json
{
  "id": "rule1_debug",
  "sql": "SELECT * FROM mockStream",
  "actions": [
    {
      "log": {
      }
    }
  ]
}
```

If your filer uses calculated data as the condition, try to create another rule to print out all related data. For
example, `SELECT * FROM mockStream WHERE temperature - lag(temperture) > 1`. The lag(temperature) is derived data. You
can create a debug rule to print out the lag(temperature) to see if it is as expected.

## End-to-end Debugging

We are going to write a simple rule that reads data from a stream and sends it to a sink if the temperature is increased
more then 1 degree. We'll use all the debugging techniques to make sure the rule is working as expected.

Firstly, we need to create a stream that will be used as the data source.

```http request
###
POST http://{{host}}/streams
Content-Type: application/json

{"sql":"CREATE STREAM mockStream() WITH (DATASOURCE=\"data/mock\", FORMAT=\"json\", TYPE=\"mqtt\");"}
```

We should receive a response with status code 200 and successfully create the stream. The stream is **schemaless** and
will subscribe to MQTT topic `data/mock` to receive data. In the experiment, we assume the data is
like: `{"temperature": 10, "humidity": 20}`.

### V1: Rule with syntax error

Our first version is written out and submit by REST API.

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT temperature, humidity FROM mockStream WHERE temprature - laig(temperature) > 1",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://yourserver:1883",
        "topic": "result"
      }
    }
  ]
}
```

We should receive a response with status code 400 and the error message should be like:

```text
HTTP/1.1 400 Bad Request

Create rule error: Invalid rule json: Parse SQL SELECT temperature, humidity FROM mockStream WHERE temprature - laig(temperature) > 1 error: function laig not found.
```

The error message is clear that we use an inexisted function named `laig`. We can fix the typo in the rule.

### V2: Rule is not running

After fixing the typo, we submit the rule again.

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT temperature, humidity FROM mockStream WHERE temprature - lag(temperature) > 1",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://yourserver:1883",
        "topic": "result"
      }
    }
  ]
}
```

This time, the rule is created successfully. However, we do not receive any data on the result topic. Let's diagnose!

Firstly, we should check the rule status:

```http request
###
GET http://{{host}}/rules/rule1/status
```

If your MQTT broker is not started yet, we may receive a response like:

```json
{
  "status": "stopped",
  "message": "Stopped: found error when connecting for tcp://yourserver:1883: network Error : dial tcp: lookup syno1.home: no such host."
}
```

The message is clear that the MQTT broker address is not accessible. We should change the broker address in the sink
setting, make sure the broker has started and check the rule status again. After the broker is running, restart the rule
by REST API:

```http request
###
POST http://{{host}}/rules/rule1/start
```

Then check the rule status again. If the rule is running, we should receive a response like:

```json
{
  "status": "running",
  "source_mockStream_0_records_in_total": 0,
  "source_mockStream_0_records_out_total": 0,
  "source_mockStream_0_process_latency_us": 0,
  "source_mockStream_0_buffer_length": 0,
  "source_mockStream_0_last_invocation": 0,
  "source_mockStream_0_exceptions_total": 0,
  "source_mockStream_0_last_exception": "",
  "source_mockStream_0_last_exception_time": 0,
  "op_2_analytic_0_records_in_total": 0,
  "op_2_analytic_0_records_out_total": 0,
  "op_2_analytic_0_process_latency_us": 0,
  "op_2_analytic_0_buffer_length": 0,
  "op_2_analytic_0_last_invocation": 0,
  "op_2_analytic_0_exceptions_total": 0,
  "op_2_analytic_0_last_exception": "",
  "op_2_analytic_0_last_exception_time": 0,
  "op_3_filter_0_records_in_total": 0,
  "op_3_filter_0_records_out_total": 0,
  "op_3_filter_0_process_latency_us": 0,
  "op_3_filter_0_buffer_length": 0,
  "op_3_filter_0_last_invocation": 0,
  "op_3_filter_0_exceptions_total": 0,
  "op_3_filter_0_last_exception": "",
  "op_3_filter_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 0,
  "op_4_project_0_records_out_total": 0,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": 0,
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": 0,
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```

This time the rule is running, just not receiving data yet. Let's send some data to the `mockStream` topic:

```json
{
  "temperature": 10,
  "humidity": 20
}
```

Then check the rule status again. The metrics have no change, the `source_mockStream_0_records_in_total` is still 0
which means the rule is not receiving data. This is likely a problem in source side. Let's check our source
configuration, in this example, check the MQTT broker and topic configuration. Ah, we configure the topic to `data/mock`
in the stream definition, but we were sending to `mockStream` topic thus the rule didn't receive data.

Let's send the data to `data/mock`. This time, we should have received data on the metric.

```json
{
  "status": "running",
  "source_mockStream_0_records_in_total": 1,
  "source_mockStream_0_records_out_total": 1,
  "source_mockStream_0_process_latency_us": 753,
  "source_mockStream_0_buffer_length": 0,
  "source_mockStream_0_last_invocation": "2023-05-31T15:49:32.997547",
  "source_mockStream_0_exceptions_total": 0,
  "source_mockStream_0_last_exception": "",
  "source_mockStream_0_last_exception_time": 0,
  "op_2_analytic_0_records_in_total": 1,
  "op_2_analytic_0_records_out_total": 1,
  "op_2_analytic_0_process_latency_us": 0,
  "op_2_analytic_0_buffer_length": 0,
  "op_2_analytic_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_2_analytic_0_exceptions_total": 0,
  "op_2_analytic_0_last_exception": "",
  "op_2_analytic_0_last_exception_time": 0,
  "op_3_filter_0_records_in_total": 1,
  "op_3_filter_0_records_out_total": 0,
  "op_3_filter_0_process_latency_us": 0,
  "op_3_filter_0_buffer_length": 0,
  "op_3_filter_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_3_filter_0_exceptions_total": 0,
  "op_3_filter_0_last_exception": "",
  "op_3_filter_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 0,
  "op_4_project_0_records_out_total": 0,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": 0,
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": 0,
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```

### V3: Diagnose the filter

Let's send the second data to the `data/mock` topic:

```json
{
  "temperature": 15,
  "humidity": 25
}
```

The temperature increases 5 which meets the where condition. But we are still not receiving data on the result topic.
How to diagnose this? First take a look at the metrics:

```json
{
  "status": "running",
  "source_mockStream_0_records_in_total": 2,
  "source_mockStream_0_records_out_total": 2,
  "source_mockStream_0_process_latency_us": 753,
  "source_mockStream_0_buffer_length": 0,
  "source_mockStream_0_last_invocation": "2023-05-31T15:49:32.997547",
  "source_mockStream_0_exceptions_total": 0,
  "source_mockStream_0_last_exception": "",
  "source_mockStream_0_last_exception_time": 0,
  "op_2_analytic_0_records_in_total": 2,
  "op_2_analytic_0_records_out_total": 2,
  "op_2_analytic_0_process_latency_us": 0,
  "op_2_analytic_0_buffer_length": 0,
  "op_2_analytic_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_2_analytic_0_exceptions_total": 0,
  "op_2_analytic_0_last_exception": "",
  "op_2_analytic_0_last_exception_time": 0,
  "op_3_filter_0_records_in_total": 2,
  "op_3_filter_0_records_out_total": 0,
  "op_3_filter_0_process_latency_us": 0,
  "op_3_filter_0_buffer_length": 0,
  "op_3_filter_0_last_invocation": "2023-05-31T15:50:10.9103",
  "op_3_filter_0_exceptions_total": 0,
  "op_3_filter_0_last_exception": "",
  "op_3_filter_0_last_exception_time": 0,
  "op_4_project_0_records_in_total": 0,
  "op_4_project_0_records_out_total": 0,
  "op_4_project_0_process_latency_us": 0,
  "op_4_project_0_buffer_length": 0,
  "op_4_project_0_last_invocation": 0,
  "op_4_project_0_exceptions_total": 0,
  "op_4_project_0_last_exception": "",
  "op_4_project_0_last_exception_time": 0,
  "sink_mqtt_0_0_records_in_total": 0,
  "sink_mqtt_0_0_records_out_total": 0,
  "sink_mqtt_0_0_process_latency_us": 0,
  "sink_mqtt_0_0_buffer_length": 0,
  "sink_mqtt_0_0_last_invocation": 0,
  "sink_mqtt_0_0_exceptions_total": 0,
  "sink_mqtt_0_0_last_exception": "",
  "sink_mqtt_0_0_last_exception_time": 0
}
```

From the metrics, we know the data are successfully ingested and flow to filter operator, but all are filtered out. This
is not expected, how to diagnose next? We can either enable debug log to see the data flow in the massive log, please
read "3. Check the debug logs" in the debugging tips section; or create a debug rule to learn the calculated data in the
filter operator.

In this example, we can create a debug rule like below:

```http request
###
POST http://{{host}}/rules
Content-Type: application/json

{
  "id": "ruleDebug",
  "sql": "SELECT temperature, humidity, temprature - lag(temperature) as diff FROM mockStream",
  "actions": [
    {
      "mqtt": {
        "server": "{{yourhost}}",
        "topic": "debug"
      }
    }
  ]
}
```

In the debug rule, we remove the `WHERE` clause, and copy its condition `temprature - lag(temperature)` to the `SELECT`
clause, which will print out for every input. We can check the printed value and see why it does not meet the condition.

Let's restart both rules, and send the two data to `data/mock` topic over again.

Check the result of `ruleDebug`, we'll find:

```json lines
{
  "temperature": 15,
  "humidity": 20
}
{
  "temperature": 20,
  "humidity": 25
}
```

We expect a `diff`, but it is not printed which means it is `nil`. This indicates we need to check the
condition `temprature - lag(temperature)`. Look into it closely; we'll find we have a typo `temprature` which should
be `temperature`. This is a common mistake when the stream is schemaless! It is not easy to find this typo by SQL parser
as in schemaless mode, the SQL parser cannot know which field is invalid. So, we need to be careful when writing SQL in
schemaless mode.

### V4: Finally correct

Let's correct the typo by updating the rule.

```http request
###
PUT http://{{host}}/rules/rule1
Content-Type: application/json

{
  "id": "rule1",
  "sql": "SELECT temperature, humidity FROM mockStream WHERE temperature - lag(temperature) > 1",
  "actions": [
    {
      "mqtt": {
        "server": "{{yourhost}}",
        "topic": "result"
      }
    }
  ]
}
```

The rule will be restarted and the metrics will be reset. Let's send the data to the `data/mock` topic from the
beginning:

```json lines
{
  "temperature": 15,
  "humidity": 20
}
{
  "temperature": 20,
  "humidity": 25
}
```

Finally, we'll receive the data on the `result` topic when condition met:

```json
{
  "temperature": 20,
  "humidity": 25
}
```

## Summary

In this tutorial, we learned how to diagnose a rule from the metrics, logs and debug rules. We also have a step-by-step
guide to create a rule and debug it. Hope this tutorial can help you to diagnose your rules.
