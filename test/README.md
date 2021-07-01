## Overview

eKuiper FVT (functional verification tests)  covers following scenarios. 

- Basic functions of HTTP REST-API 
- Basic functions of CLI
- Complex end-2-end scenario for eKuiper source, processing and sink

The scenarios will be invoked automatically in Github actions with any new code commit or push request. Another Raspberry Pi continouly integration environment will also be ready for running test cases in ARM environment. So if receives any failed FVT running, please re-check the code or update the scripts if necessary.

eKuiper project uses JMeter for writing the scripts for following reasons,

- Easy to write testcases for HTTP REST-API & CLI
- Capabilities of publish and subscribe MQTT message. The eKuiper end-2-end scenarios requires MQTT client for pub/sub message, and JMeter provides a unified approach for supporting such functions
- Capabilities of writing complex content assertions. Besides some simple ways of content assertions, JMeter also provides ``BeanShell Assertion``, which can be used for extract and process complex message contents. 

## Run script in local development environment

**Prepare JMeter**

eKuiper uses JMeter for FVT test scenarios, includes REST-API, CLI and end to end test scenarios. 

- Install JRE - requires JRE 8+
- Download and extract [JMeter](http://jmeter.apache.org/download_jmeter.cgi). 

**Install MQTT broker**

Because test scripts uses MQTT broker for source and sink of eKuiper rule, an MQTT broker is required for running the scripts. If you use a broker that cannot be accessed from ``tcp://127.0.0.1:1883``, you should modify the script and specify your MQTT broker address.

- Modify servers to your MQTT broker address in eKuiper configuration file ``etc/mqtt_source.yaml``. 
- Modify the script file that you want to run.
  - ``mqtt_srv``: The default value is ``127.0.0.1``, you need to update it if you have a different broker. Refer to below screenshot, ``Test Plan > User Defined Variables > mqtt_srv``.
    
    ![jmeter_variables](resources/jmeter_variables.png)
    
  - If you run test eKuiper server at another address or port, you need also change below two config.
    - ``Test Plan > User Defined Variables > srv``: The eKuiper server address, by default is at ``127.0.0.1``.
    - ``Test Plan > User Defined Variables > rest_port``: The eKuiper server RestAPI port, by default is ``9081``, please change it if running eKuiper at a different port.

**Run JMeter**

For most of scripts, you can just start JMeter by default way, such as ``bin/jmeter.sh`` in Mac or Linux. But some of scripts need to pass some parameters before running them. Please refer to below for detailed. Please make sure you start MQTT broker & eKuiper before running the tests.

## Scenarios

- [Basic stream test](streams_test.jmx)

  The script tests the basic steps for stream operations, include both API & CLI.

  - Create/Delete/Describe/Show stream for RestAPI
  - Create/Delete/Describe/Show stream for CLI

  The script need to be told about the location of eKuiper install directory, so script knows where to invoke eKuiper CLI.

  - Specify the ``base`` property in the JMeter command line, the ``base`` is where eKuiper installs. Below is command for starting JMeter.

    ```shell
    bin/jmeter.sh -Dbase="/opt/kuiper"
    ```

- [Basic rules test](rule_test.jmx)
  
  The script tests stream and rule operations.
  
  - Create a stream with MQTT source, and then call rule management
    - Create/Delete/Describe/Show rule for RestAPI
    - Create/Delete/Describe/Show rule for CLI
  - Delete stream definition at the last step of test scenario
  
  The script need to be told about the location of eKuiper install directory, so script knows where to invoke eKuiper CLI.
  
  - Specify the ``base`` property in the JMeter command line, the ``base`` is where eKuiper installs. 
  - Specify the ``fvt`` property in the JMeter command line, the ``fvt`` is where you develop eKuiper, script will read rule file  ``test/rule1.txt`` from the location.
  
  - Modify ``mqtt.server`` to your MQTT broker address in file ``test/rule1.txt``.
  
  - So below is command for starting JMeter.
  
    ```shell
    bin/jmeter.sh -Dbase="/opt/kuiper" -Dfvt="/Users/rockyjin/Downloads/workspace/edge/src/ekuiper"
    ```
  
- [Select all (*) records rule test](select_all_rule.jmx)

  The scenario tests a rule that select all of records from a stream.

  - Stream source is MQTT, and JSON data are sent to an MQTT topic by JMeter. The sent data are read from file ``iot_data.txt``, where the 1st column is ``device_id``, the 2nd column is ``temperature``, the 3rd column is ``humidity``. There are totally 10 records in the file.
  - The processing SQL is ``SELECT * FROM demo``,  so all of data will be processed and sent to sinks.
  - There are two sinks for the rule, one is log, and another is MQTT sink. So result will be sent to those sinks.
  - Another JMeter mock-up user subscribes MQTT result topic. JMeter validates message number and  content sent by the rule. If the record cotent is not correct then JMeter response assertion will be failed. If record number is not correct, the script will not be stopped, until CI (continuous integration) pipeline kills it with timeout settings. If you run the script in local, you'll have to stop the test manually.

- [Select records with condition](select_condition_rule.jmx)

  This scenario test is very similar to the last one, except the rule filters the record with a condition.

  - The processing SQL is ``SELECT * FROM demo WHERE temperature > 30``, so all of the data that with temperature less than 30 will be fitered. The script read data from  file ``iot_data.txt``, totally 10 records.
  - Another JMeter mock-up user subscribes MQTT result topic, and expected result are saved in file ``select_condition_iot_data.txt``. If the record cotent is not correct then JMeter response assertion will be failed. If record number is not correct, the script will not be stopped, until CI (continuous integration) pipeline kills it with timeout settings. If you run the script in local, you'll have to stop the test manually.

- [Aggregation rule](select_aggr_rule.jmx)

  The script automated steps described in [this blog](https://www.emqx.io/blog/lightweight-edge-computing-emqx-kuiper-and-aws-iot-hub-integration-solution), except for the sink target changes to local EMQ broker (not AWS IoT Hub). 

  - The processing SQL is as following.

    ```sql
    SELECT avg(temperature) AS t_av, max(temperature) AS t_max, min(temperature) AS t_min, COUNT(*) As t_count, split_value(mqtt(topic), "/", 1) AS device_id FROM demo GROUP BY device_id, TUMBLINGWINDOW(ss, 5)
    ```

  - Another JMeter mock-up user subscribes MQTT result topic, and it waits for 15 seconds to get all of analysis result arrays.  With the beanshell assertion, it calculates total number of ``t_count`` for device 1 & 2. If the number is not correct, then it fails. 

- [Change rule status & get rule metrics](change_rule_status.jmx)

  This script creates stream and rule, then get metrics of rule, and assert message number processed in stream processing line. Additionally, script will stop, start or restart the rule, and verify the metric value of rule. 

  Another JMeter mock-up user subscribes MQTT result topic, and assert message number and contents.

- [Change stream definition and restart rule](change_stream_rule.jmx)

  The script tests scenarios for following cases,

  - Use the SQL to select a field that is not existed in stream definition, it returns ``[{}]``.
  - stream definition changed, and rule works well after restarting it.

  Another JMeter mock-up user subscribes MQTT result topic, and assert message number and contents.

- [Aggregation with ORDER BY](select_aggr_rule_order.jmx)

  - The script adds ``ORDER BY`` statement based on ``Aggregation rule``.

    ```sql
    SELECT temperature, humidity, split_value(mqtt(topic), "/", 1) AS device_id FROM demo GROUP BY TUMBLINGWINDOW(ss, 10) ORDER BY device_id DESC, temperature
    ```

  - Another JMeter mock-up user subscribes MQTT result topic, and assert the order for device_id field is descending, and temperature is ascending.

- [EdgeX source with condition](select_edgex_condition_rule.jmx)

  The test script is used for testing [eKuiper EdgeX source](../docs/en_US/rules/sources/edgex.md). To run the script, 

  - An EdgeX message bus publish tool should be compiled and run during running test.

    ```shell
    # go build -o test/edgex/pub test/edgex/pub.go
    ```

  - Run the JMeter with following command, and specify the ``fvt`` property in the JMeter command line, the ``fvt`` is where you develop eKuiper, script will search ``test/edgex/pub`` from the location.

    ```shell
    bin/jmeter.sh -Dfvt="/Users/rockyjin/Downloads/workspace/edge/src/ekuiper"
    ```

  - The processing SQL is ``SELECT * FROM demo WHERE temperature > 30``, so all of the data that with temperature less than 30 will be fitered. 
  
  - Another JMeter mock-up user subscribes MQTT result topic, and assert message number and contents.
  
- [Multiple EdgeX source configurations](test/select_edgex_another_bus_rule.jmx)

  The test script is used for testing specifying another EdgeX source configurations in eKuiper.

  - In the ``edgex.yaml`` configuration file, below additional configurations are specified.

  ```yaml
  application_conf: #Conf_key
    protocol: tcp
    server: localhost
    port: 5571
    topic: application
  ```

  - In the create stream statement, test script uses ``CONF_KEY`` keyword to use overrided configuration value that specified in ``edgex.yaml``.

  ```sql
  CREATE STREAM application () WITH (FORMAT="JSON", TYPE="edgex", CONF_KEY = "application_conf")
  ```

  - As same steps that required in the ``select_edgex_condition_rule.jmx``, EdgeX value descriptor service & message bus publish tool should be ready.

- [EdgeX message bus sink](edgex_sink_rule.jmx)

  The test script verifies EdgeX message bus sink.  Only one message meet the condition of created rule, and it will be sent to EdgeX message bus sink.

  As with the previous 2 testcases, besides to prepare ``pub`` application, another ``sub`` application should also be prepared.

  ```shell
  # go build -o test/edgex/sub/sub test/edgex/sub/sub.go 
  ```

- [EdgeX array data type](edgex_array_rule.jmx)

  The test script verifies EdgeX array data type support. The rule uses JSON expression in both `SELECT` and `WHERE` clause. The sink result is sent to MQTT broker, and it verifies the project result in sampler assertions.

  This test script, you need to prepare ``pub`` application.

- [An end to end plugin test](plugin_end_2_end.jmx)
  The script is an end-2-end plugin test. It requires a mock http server, and also a plugin.
    ```shell
    # go build -o test/plugins/service/http_server test/plugins/service/server.go 
    ```

- [Pull HTTP test](http_pull_rule.jmx)
  
  The test script verifies HTTP pull source. It sends request to a [server](test/plugins/service/server.go). The script set incremental to true, so it will compare with last result; If response of two requests are the same, then will skip sending out the result.
  This script also requires to run [server](test/plugins/service/server.go), please refer to last testcase for how to compile and run.
  
- [Binary data type test](http_pull_rule.jmx)
  
  This script verifies the binary data support. The rule consumes the binary image data sent from MQTT broker, and then processed by the rule, finally the image data will be sent back to MQTT broker again. The script verifies BASE64 of image that produced by eKuiper rule engine. 
  
  - `binary_image_hex.txt`: the data file of image.
  - `binary_image_base64.txt`: the BASE64 file of image.

- [Static table test](table_static.jmx)

  This script verifies the batch table support. 
  - Create a MQTT stream. The sent data are read from file `iot_data_id.txt` which defines an id, temperature and humidity.
  - Create a table whose type is file which reads `lookup.json` as the content. It is like a dictionary which maps the name and size to each id.
  - The rule join the stream and table by id to lookup the name and filter by temperature.
  - The result data will be sent back to MQTT broker again and compared with `table_static_result_data.txt`.

- [Continuous table test](table_cont.jmx)

  This script verifies the continuous updated table support. Unlike the static table, the continuous table will be updated once received new data.
  - Create a MQTT stream with topic named `iot`. The sent data are read from file `iot_data_multi_topics.txt` when the topic name is matched which defines temperature and humidity.
  - Create a table whose type is mqtt with topic name `state`. The sent data is also from file `iot_data_multi_topics.txt` when the topic name is matched which defines the state. The table records the latest state from the `state` topic which is working like a control topic to switch on or off of the processing.
  - The rule join the stream and table filter by both the state from the table and temperature from the stream.  
  - The result data will be sent back to MQTT broker again and compared with `table_cont_result_data.txt`.

- [Shared source instance test](shared_source_rules.jmx)

  This script verifies the shared source instances across rules. By specifying shared property to true in the stream definition, all rules using the stream will all share the same source instance for better performance.
  - Create a MQTT stream with `SHARED` option true. The sent data are read from file `iot_data.txt` which defines temperature and humidity.
  - Create 3 rules which all select from the stream just created with different where filter. They should share the same mqtt source instance.
  - Send data to the stream. Make sure all 3 rules receive the exact same copy of data but conduct different filter by checking the result.
  - The result data will be sent back to MQTT broker again and compared with `select_condition_iot_data.txt`, `select_condition_iot_data2.txt` and `select_condition_iot_data3.txt` respectively.