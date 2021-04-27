

- Build the ``pub.go``, this is used for simulating the data.
    ```shell
    # go build -o fvt_scripts/edgex/benchmark/pub fvt_scripts/edgex/benchmark/pub.go
    ```

- Update edgex configuration. Update ``Server`` configuration to the address where you run ``pub`` in the 1st step.  
  ```yaml
  default:
    protocol: tcp
    server: 172.31.1.144
    port: 5563
    topic: events
  ```

- The rule is listed as following, save the rule as ``rule.txt``.

   ```json
    {
      "sql": "SELECT * from demo where temperature>50",
      "actions": [
        {
          "nop": {
            "log": false
          }
        }
      ]
    }
   ```
  
- Create stream ``bin/kuiper create stream demo'() WITH (FORMAT="JSON", TYPE="edgex")'``

- Create rule ``bin/kuiper create rule rule1 -f rule.txt``. To add another rule, just change the rule name of command, then deploy another rule, e,g, ``bin/kuiper create rule rule2 -f rule.txt``

- Run ``pub`` application ``./pub 1000000``, below is an example.
  ```shell script
  ubuntu@ip-172-31-1-144:~$ ./pub 1000000
  elapsed 174.924363s
  ```
  
  Check the status of rule,
  ```shell script
  ubuntu@ip-172-31-5-85:/tmp/kuiper-master/_build/kuiper--linux-amd64$ bin/kuiper getstatus rule rule1
    Connecting to 127.0.0.1:20498...
    {
      "source_demo_0_records_in_total": 1000000,
      "source_demo_0_records_out_total": 1000000,
      "source_demo_0_exceptions_total": 0,
      "source_demo_0_process_latency_ms": 0,
      "source_demo_0_buffer_length": 0,
      "source_demo_0_last_invocation": "2020-04-10T04:26:15.51329",
      "op_preprocessor_demo_0_records_in_total": 1000000,
      "op_preprocessor_demo_0_records_out_total": 1000000,
      "op_preprocessor_demo_0_exceptions_total": 0,
      "op_preprocessor_demo_0_process_latency_ms": 0,
      "op_preprocessor_demo_0_buffer_length": 0,
      "op_preprocessor_demo_0_last_invocation": "2020-04-10T04:26:15.513371",
      "op_filter_0_records_in_total": 1000000,
      "op_filter_0_records_out_total": 100000,
      "op_filter_0_exceptions_total": 0,
      "op_filter_0_process_latency_ms": 0,
      "op_filter_0_buffer_length": 0,
      "op_filter_0_last_invocation": "2020-04-10T04:26:15.513391",
      "op_project_0_records_in_total": 100000,
      "op_project_0_records_out_total": 100000,
      "op_project_0_exceptions_total": 0,
      "op_project_0_process_latency_ms": 0,
      "op_project_0_buffer_length": 0,
      "op_project_0_last_invocation": "2020-04-10T04:26:15.513468",
      "sink_nop_0_0_records_in_total": 100000,
      "sink_nop_0_0_records_out_total": 100000,
      "sink_nop_0_0_exceptions_total": 0,
      "sink_nop_0_0_process_latency_ms": 0,
      "sink_nop_0_0_buffer_length": 1,
      "sink_nop_0_0_last_invocation": "2020-04-10T04:26:15.513501"
    }
  ubuntu@ip-172-31-5-85:/tmp/kuiper-master/_build/kuiper--linux-amd64$ bin/kuiper getstatus rule rule2
    Connecting to 127.0.0.1:20498...
    {
      "source_demo_0_records_in_total": 1000000,
      "source_demo_0_records_out_total": 1000000,
      "source_demo_0_exceptions_total": 0,
      "source_demo_0_process_latency_ms": 0,
      "source_demo_0_buffer_length": 0,
      "source_demo_0_last_invocation": "2020-04-10T04:26:15.514621",
      "op_preprocessor_demo_0_records_in_total": 1000000,
      "op_preprocessor_demo_0_records_out_total": 1000000,
      "op_preprocessor_demo_0_exceptions_total": 0,
      "op_preprocessor_demo_0_process_latency_ms": 0,
      "op_preprocessor_demo_0_buffer_length": 0,
      "op_preprocessor_demo_0_last_invocation": "2020-04-10T04:26:15.514631",
      "op_filter_0_records_in_total": 1000000,
      "op_filter_0_records_out_total": 100000,
      "op_filter_0_exceptions_total": 0,
      "op_filter_0_process_latency_ms": 0,
      "op_filter_0_buffer_length": 0,
      "op_filter_0_last_invocation": "2020-04-10T04:26:15.514635",
      "op_project_0_records_in_total": 100000,
      "op_project_0_records_out_total": 100000,
      "op_project_0_exceptions_total": 0,
      "op_project_0_process_latency_ms": 0,
      "op_project_0_buffer_length": 0,
      "op_project_0_last_invocation": "2020-04-10T04:26:15.514639",
      "sink_nop_0_0_records_in_total": 100000,
      "sink_nop_0_0_records_out_total": 100000,
      "sink_nop_0_0_exceptions_total": 0,
      "sink_nop_0_0_process_latency_ms": 0,
      "sink_nop_0_0_buffer_length": 1,
      "sink_nop_0_0_last_invocation": "2020-04-10T04:26:15.514652"
    }
  ```
Below is the system usage screenshot,

  ![](system_usage.png)
