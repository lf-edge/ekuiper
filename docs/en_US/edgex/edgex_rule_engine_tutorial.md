

[ToC]

# EdgeX rule engine tutorial

## Overview

In EdgeX Geneva, [EMQ X Kuiper](https://github.com/emqx/kuiper) is used as implementation of rule engine. Before diving into this tutorial, let's spend a little time on learning basic knowledge of Kuiper. Kuiper is an edge lightweight IoT data analytics / streaming software implemented by Golang, and it can be run at all kinds of resource constrained edge devices. Kuiper rules are based on ``Source``, ``SQL`` and ``Sink``.

- Source: The data source of streaming data, such as data from MQTT broker. In EdgeX scenario, the data source is EdgeX message bus, which could be ZeroMQ or MQTT broker.
- SQL: SQL is where you specify the business logic of streaming data process. Kuiper provides SQL-like statements to allow you to extract, filter & transform data. 
- Sink: Sinks is ued for sending analysis result to a specified target. For example, send analysis result to another MQTT broker, or an HTTP rest address.

![](../../resources/arch.png)

Following three steps are required for using Kuiper.

- Create a stream, where you specify the data source.
- Write a rule.
  - Write a SQL for data analysis
  - Specify a sink target for saving analysis result
- Deploy and run rule.

The tutorial demonstrates how to use Kuiper to process the data from EdgeX message bus.

## Kuiper EdgeX integration

EdgeX uses [message bus](https://github.com/edgexfoundry/go-mod-messaging) to exchange information between different micro services. It contains the abstract message bus interface and an implementation for ZeroMQ and MQTT. The integration work for Kuiper & EdgeX includes following 3 parts.

- An EdgeX message bus source is extended to  support consuming data from EdgeX message bus.  

- To analyze the data, Kuiper need to know data types that passed through it. Generally, user would be better to specify data schema for analysis data when a stream is created. Such as in below, a ``demo`` stream has a field named ``temperature`` field. It is very similar to create table schema in relational database system. After creating the stream definition, Kuiper can perform type checking during compilation or runtime, and invalid SQLs or data will be reported to user.

  ```shell
  CREATE STREAM demo (temperature bigint) WITH (FORMAT="JSON"...)
  ```

  However, since data type definitions are already specified in EdgeX ``Core contract Service`` , and to improve the using experience, user are NOT necessary to specify data types when creating stream. Kuiper source tries to load all of ``value  descriptors`` from ``Core contract Service`` during initialization, then if with any data sending from message bus, it will be converted into [corresponding data types](../rules/sources/edgex.md).

- An EdgeX message bus sink is extended to support send analysis result back to EdgeX Message Bus. User can also choose to send analysis result to RestAPI, Kuiper already supported it. 

![](arch_light.png)

## Start to use

### Pull Kuiper Docker and run

It's recommended to use Docker, since related dependency libraries (such ZeroMQ lib) are already installed in Docker images.

```shell
docker pull emqx/kuiper:0.2.1
```

<u>TODO: After offcially releasing of EdgeX Geneva, the Kuiper docker image will be pulled automatically by EdgeX docker composer files. The command will be updated by then.</u>  

**Run Docker**

```
docker run -d --name kuiper emqx/kuiper:0.2.1
```

If the docker instance is failed to start, please use ``docker logs kuiper`` to see the log files.

### Create a device service

In this tutorial, we use a very simple mock-up device service. Please follow the steps in [this doc](https://fuji-docs.edgexfoundry.org/Ch-GettingStartedSDK-Go.html) to develop and run the random number service.  

### Create a stream

The next step is to create a stream that can consuming data from EdgeX message bus. Run following command to enter the running Kuiper docker instance.

```shell
docker exec -it kuiper /bin/sh
```

Use following command to create a stream named ``demo``.

```shell
bin/cli create stream demo'() WITH (FORMAT="JSON", TYPE="edgex")'
```

Kuiper also provides [RestAPI for streams and rules management](../restapi/overview.md), so you can also use any HTTP client tools (such as ``curl``) to create the streams.

You maybe curious about how Kuiper knows the message bus IP address & port, because such information are not specified in ``CREATE STREAM`` statement. Those configurations are managed in ``etc/sources/edgex.yaml`` , you can type ``cat etc/sources/edgex.yaml`` command to take a look at the contents of file.  If you have different server, ports & service server configurations, please update it accordingly.

```yaml
#Global Edgex configurations
default:
  protocol: tcp
  server: localhost
  port: 5570
  topic: events
  serviceServer: http://localhost:10080
.....  
```

For more detailed information of configuration file, please refer to [this doc](../rules/sources/edgex.md).

### Create a rule

Let's create a rule that send result data to file, for detailed information of file sink, please refer to [this link](../plugins/sinks/file.md). Create a rule file with any tools, and copy following contents into it. Let's say the file name is ``rule.txt``.  So the below rule will filter all of ``randomnumber`` that is less than 31. The sink result will be published to a public MQTT broker ``broker.emqx.io``. 

```
{
  "sql": "SELECT * from demo where randomnumber > 30",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "result",
        "clientId": "demo_001"
      }
    }
  ]
}
```

If you want to send analysis result to another sink, please refer to [other sinks](../rules/overview.md#actions) that supported in Kuiper.

In the running Kuiper instance, and execute following command.

```shell
# bin/cli create rule rule1 -f rule.txt
Connecting to 127.0.0.1:20498...
Creating a new rule from file rule.txt.
Rule rule1 was created, please use 'cli getstatus rule $rule_name' command to get rule status.
```

Now you can also take a look at the log file under ``log/stream.log``, see detailed info of rule. 

```
time="2020-03-19T10:23:40+08:00" level=info msg="open source node 1 instances" rule=rule1
time="2020-03-19T10:23:40+08:00" level=info msg="Connect to value descriptor service at: http://localhost:48080/api/v1/valuedescriptor \n"
time="2020-03-19T10:23:40+08:00" level=info msg="Use configuration for edgex messagebus {{ 0 } {localhost 5563 tcp} zero map[]}\n"
time="2020-03-19T10:23:40+08:00" level=info msg="Start source demo instance 0 successfully" rule=rule1
time="2020-03-19T10:23:40+08:00" level=info msg="The connection to edgex messagebus is established successfully." rule=rule1
time="2020-03-19T10:23:40+08:00" level=info msg="Successfully subscribed to edgex messagebus topic events." rule=rule1
time="2020-03-19T10:23:40+08:00" level=info msg="The connection to server tcp://broker.emqx.io:1883 was established successfully" rule=rule1
```

### Monitor analysis result

Since all of the analysis result are published to  ``tcp://broker.emqx.io:1883``, so you can just use below ``mosquitto_sub`` command to monitor the result. You can also use other [MQTT client tools](https://www.emqx.io/blog/mqtt-client-tools).

```shell
# mosquitto_sub -h broker.emqx.io -t result
[{"randomnumber":81}]
[{"randomnumber":87}]
[{"randomnumber":47}]
[{"randomnumber":59}]
[{"randomnumber":81}]
...
```

You'll find that only those randomnumber larger than 30 will be published to ``result`` topic.

You can also type below command to look at the rule execution status.

```shell
# bin/cli getstatus rule rule1
Connecting to 127.0.0.1:20498...
{
  "source_demo_0_records_in_total": 29,
  "source_demo_0_records_out_total": 29,
  "source_demo_0_exceptions_total": 0,
  "source_demo_0_process_latency_ms": 0,
  "source_demo_0_buffer_length": 0,
  "source_demo_0_last_invocation": "2020-03-19T10:30:09.294337",
  "op_preprocessor_demo_0_records_in_total": 29,
  "op_preprocessor_demo_0_records_out_total": 29,
  "op_preprocessor_demo_0_exceptions_total": 0,
  "op_preprocessor_demo_0_process_latency_ms": 0,
  "op_preprocessor_demo_0_buffer_length": 0,
  "op_preprocessor_demo_0_last_invocation": "2020-03-19T10:30:09.294355",
  "op_filter_0_records_in_total": 29,
  "op_filter_0_records_out_total": 21,
  "op_filter_0_exceptions_total": 0,
  "op_filter_0_process_latency_ms": 0,
  "op_filter_0_buffer_length": 0,
  "op_filter_0_last_invocation": "2020-03-19T10:30:09.294362",
  "op_project_0_records_in_total": 21,
  "op_project_0_records_out_total": 21,
  "op_project_0_exceptions_total": 0,
  "op_project_0_process_latency_ms": 0,
  "op_project_0_buffer_length": 0,
  "op_project_0_last_invocation": "2020-03-19T10:30:09.294382",
  "sink_sink_mqtt_0_records_in_total": 21,
  "sink_sink_mqtt_0_records_out_total": 21,
  "sink_sink_mqtt_0_exceptions_total": 0,
  "sink_sink_mqtt_0_process_latency_ms": 0,
  "sink_sink_mqtt_0_buffer_length": 1,
  "sink_sink_mqtt_0_last_invocation": "2020-03-19T10:30:09.294423"
}
```

### Summary

In this tutorial,  we introduce a very simple use of EdgeX Kuiper rule engine. If you want to explore more powerful features of EMQ X Kuiper, you can refer to below resources.

- [Kuiper Github code repository](https://github.com/emqx/kuiper/)
- [Kuiper reference guide](https://github.com/emqx/kuiper/blob/edgex/docs/en_US/reference.md)

If having any issues regarding to use of Kuiper rule engine, you can open issues in EdgeX or Kuiper Github respository.

