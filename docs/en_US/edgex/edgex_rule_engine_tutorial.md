# EdgeX rule engine tutorial

## Overview

In EdgeX Geneva, [EMQ X Kuiper - an SQL based rule engine](https://github.com/emqx/kuiper) is integrated with EdgeX. Before diving into this tutorial, let's spend a little time on learning basic knowledge of Kuiper. Kuiper is an edge lightweight IoT data analytics / streaming software implemented by Golang, and it can be run at all kinds of resource constrained edge devices. Kuiper rules are based on ``Source``, ``SQL`` and ``Sink``.

- Source: The data source of streaming data, such as data from MQTT broker. In EdgeX scenario, the data source is EdgeX message bus, which could be ZeroMQ or MQTT broker.
- SQL: SQL is where you specify the business logic of streaming data processing. Kuiper provides SQL-like statements to allow you to extract, filter & transform data. 
- Sink: Sink is ued for sending analysis result to a specified target. For example, send analysis result to another MQTT broker, or an HTTP rest address.

![](../../resources/arch.png)

Following three steps are required for using Kuiper.

- Create a stream, where you specify the data source.
- Write a rule.
  - Write a SQL for data analysis
  - Specify a sink target for saving analysis result
- Deploy and run rule.

The tutorial demonstrates how to use Kuiper to process the data from EdgeX message bus.

## Kuiper EdgeX integration

EdgeX uses [message bus](https://github.com/edgexfoundry/go-mod-messaging) to exchange information between different micro services. It contains the abstract message bus interface and an implementation for ZeroMQ & MQTT (NOTICE:  **ONLY ZeroMQ** message bus is supported in Kuiper rule engine, MQTT will be supported in later versions). The integration work for Kuiper & EdgeX includes following 3 parts. 

- An EdgeX message bus source is extended to  support consuming data from EdgeX message bus.  

- To analyze the data, Kuiper need to know data types that passed through it. Generally, user would be better to specify data schema for analysis data when a stream is created. Such as in below, a ``demo`` stream has a field named ``temperature`` field. It is very similar to create table schema in relational database system. After creating the stream definition, Kuiper can perform type checking during compilation or runtime, and invalid SQLs or data will be reported to user.

  ```shell
  CREATE STREAM demo (temperature bigint) WITH (FORMAT="JSON"...)
  ```

  However, since data type definitions are already specified in EdgeX ``Core contract Service`` , and to improve the using experience, user are NOT necessary to specify data types when creating stream. Kuiper source tries to load all of ``value descriptors`` from ``Core contract Service`` during initialization of a rule (so now if you have any updated value descriptors, you will have to **restart the rule**), then if with any data sending from message bus, it will be converted into [corresponding data types](../rules/sources/edgex.md).

- An EdgeX message bus sink is extended to support send analysis result back to EdgeX Message Bus. User can also choose to send analysis result to RestAPI, Kuiper already supported it. 

![](arch_light.png)

## Start to use

### Pull Kuiper Docker and run

It's **STRONGLY** recommended to use Docker, since related dependency libraries (such ZeroMQ lib) are already installed in Docker images.

```shell
docker pull emqx/kuiper:0.3.0
```

<u>TODO: After offcially releasing of EdgeX Geneva, the Kuiper docker image will be pulled automatically by EdgeX docker composer files. The command will be updated by then.</u>  

**Run Docker**

```
docker run -d --name kuiper emqx/kuiper:0.3.0
```

If the docker instance is failed to start, please use ``docker logs kuiper`` to see the log files.

Notice 1: The default EdgeX message bus configuration could be updated when bring-up the Docker instance.  As listed in below, override the default configurations for message bus server, port and service server address for getting value descriptors in Kuiper instance.

```shell
docker run -d --name kuiper -e EDGEX_SERVER=10.211.55.2 -e EDGEX_PORT=5563 -e EDGEX_SERVICE_SERVER=http://10.211.55.2:48080 emqx/kuiper:0.3
```

For more detailed supported Docer environment varialbles, please refer to [this link](https://hub.docker.com/r/emqx/kuiper).

*Notice 2: If you'd like to use Kuiper with EdgeX support seperately (without Docker), you could build Kuiper by yourself with ``make pkg_with_edgex`` command.*

### Create a device service

In this tutorial, we use a very simple mock-up device service. Please follow the steps in [this doc](https://fuji-docs.edgexfoundry.org/Ch-GettingStartedSDK-Go.html) to develop and run the random number service.  

### Create a stream

There are two approaches to manage stream, you can use your preferred approach.

#### Option 1: Use Rest API

The next step is to create a stream that can consume data from EdgeX message bus. Please change ``$your_server`` to Kuiper docker instance IP address.

```shell
curl -X POST \
  http://$your_server:9081/streams \
  -H 'Content-Type: application/json' \
  -d '{
  "sql": "create stream demo() WITH (FORMAT=\"JSON\", TYPE=\"edgex\")"
}'
```

For other Rest APIs, please refer to [this doc](../restapi/overview.md).

#### Option 2: Use Kuiper CLI

Run following command to enter the running Kuiper docker instance.

```shell
docker exec -it kuiper /bin/sh
```

Use following command to create a stream named ``demo``.

```shell
bin/cli create stream demo'() WITH (FORMAT="JSON", TYPE="edgex")'
```

For other command line tools, please refer to [this doc](../cli/overview.md).

------

Now the stream is created. But you maybe curious about how Kuiper knows the message bus IP address & port, because such information are not specified in ``CREATE STREAM`` statement. Those configurations are managed in ``etc/sources/edgex.yaml`` , you can type ``cat etc/sources/edgex.yaml`` command to take a look at the contents of file.  If you have different server, ports & service server configurations, please update it accordingly. As mentioned previously, these configurations could be override when bring-up the Docker instances.

```yaml
#Global Edgex configurations
default:
  protocol: tcp
  server: localhost
  port: 5563
  topic: events
  serviceServer: http://localhost:48080
.....  
```

For more detailed information of configuration file, please refer to [this doc](../rules/sources/edgex.md).

### Create a rule

Let's create a rule that send result data to an MQTT broker, for detailed information of MQTT sink, please refer to [this link](../rules/sinks/mqtt.md).  Similar to create a stream, you can also choose REST or CLI to manage rules. 

So the below rule will filter all of ``randomnumber`` that is less than 31. The sink result will be published to topic ``result`` of public MQTT broker ``broker.emqx.io``. 

#### Option 1: Use Rest API

```shell
curl -X POST \
  http://$your_server:9081/rules \
  -H 'Content-Type: application/json' \
  -d '{
  "id": "rule1",
  "sql": "SELECT * FROM demo WHERE randomnumber > 30",
  "actions": [
    {
      "mqtt": {
        "server": "tcp://broker.emqx.io:1883",
        "topic": "result",
        "clientId": "demo_001"
      }
    }
  ]
}'
```

#### Option 2: Use Kuiper CLI

You can create a rule file with any text editor, and copy following contents into it. Let's say the file name is ``rule.txt``.  

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

In the running Kuiper instance, and execute following command.

```shell
# bin/cli create rule rule1 -f rule.txt
Connecting to 127.0.0.1:20498...
Creating a new rule from file rule.txt.
Rule rule1 was created, please use 'cli getstatus rule $rule_name' command to get rule status.
```

------

If you want to send analysis result to another sink, please refer to [other sinks](../rules/overview.md#actions) that supported in Kuiper.

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

You can also type below command to look at the rule execution status. The corresponding REST API is also available for getting rule status, please check [related document](../restapi/overview.md).

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

In this tutorial,  we introduce a very simple use of EdgeX Kuiper rule engine. If having any issues regarding to use of Kuiper rule engine, you can open issues in EdgeX or Kuiper Github respository.

#### Extended Reading

- Read [EdgeX source](../rules/sources/edgex.md) for more detailed information of configurations and data type conversion.
- [How to use meta function to extract additional data from EdgeX message bus?](edgex_meta.md) There are some other information are sent along with device service, such as event created time, event id etc. If you want to use such metadata information in your SQL statements, please refer to this doc.
- [EdgeX message bus sink doc](../rules/sinks/edgex.md). The document describes how to use EdgeX message bus sink. If you'd like to send the analysis result into message bus, you are probably interested in this article. 

 If you want to explore more features of EMQ X Kuiper, please refer to below resources.

- [Kuiper Github code repository](https://github.com/emqx/kuiper/)
- [Kuiper reference guide](https://github.com/emqx/kuiper/blob/edgex/docs/en_US/reference.md)

