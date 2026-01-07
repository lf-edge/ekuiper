# Rule Test Run

When writing rules, users need to verify whether the rules are correct, and combine data to verify whether the rules can
run normally to get the expected results. This section's series of APIs are used to support the trial run of rules, so
that preliminary verification of the rules can be done without the need to tediously create rule input data.

The test rule is a temporary rule, it will not be saved on the server, and is only used for the trial run of the rule.
Test rules can only be managed using the APIs in this section. The rule running time is fixed (currently 10 minutes),
and it will automatically stop and clear after the time is exceeded. The general steps for using rule trial run are as
follows:

1. [Create a test rule](#create-a-test-rule), get the id and port of the test rule.
2. Use the id and port of the test rule to connect and listen to the WebSocket service. Its service address
   is `http://locahost:10081/test/myid` where `10081` is the port value returned in step 1, and myid is the id of the
   test rule.
3. [Start the test rule](#start-the-test-rule), wait for the test rule to run. The rule running result will be returned
   through the WebSocket service.
4. After the rule trial run ends, [delete the test rule](#delete-the-test-rule), and close the WebSocket service.

::: tip

The WebSocket service defaults to port 10081, which can be modified by the `httpServerPort` field in the `kuiper.yaml`
configuration file. Before using the test rule, please make sure that this port is accessible.

:::

## Create a Test Rule

```shell
POST /ruletest
```

Create a trial run rule, wait for it to run. This API can check syntax, ensuring the creation of an executable trial run
rule. The request body is required, the request body format is `application/json`, an example is as follows:

```json
{
  "id": "uuid",
  "sql": "select * from demo",
  "mockSource": {
    "demo": {
      "data": [
        {
          "a": 2
        },
        {
          "b": 3
        }
      ],
      "interval": 100,
      "loop": true
    },
    "demo1": {
      "data": [
        {
          "n": 2
        },
        {
          "n": 3
        }
      ],
      "interval": 200,
      "loop": true
    }
  },
  "sinkProps": {
    "dataTemplate": "xxx",
    "fields": ["abc", "test"]
  }
}
```

The request body parameters contain 4 parts:

- id: The id of the test rule, required, used for subsequent test rule management. Ensure uniqueness, it cannot be
  repeated with other test rules, otherwise the original test rule will be overwritten. This id has no association with
  the id of ordinary rules.
- sql: The sql statement of the test rule, required, used to define the syntax of the test rule.
- mockSource: The mock rule definition of the data source of the test rule, optional, used to define the input data of
  the test rule. If not defined, the real data source in SQL will be used.
- sinkProps: The definition of the sink parameters of the test rule, optional. Most of the common parameters of the sink
  can be used, such as `dataTemplate` and `fields`. If not defined, the default sink parameters will be used.

If created successfully, the return example is as follows:

```json
{
  "id": "uuid",
  "port": 10081
}
```

After the rule is created successfully, the websocket endpoint starts. Users can listen to the websocket
address `http://locahost:10081/test/uuid` to get the result output. Among them, the port and id are the above return
values.

If creation fails, the status code is 400, return error information, an example is as follows:

```json
{
  "msg": "error message here"
}
```

## Start the Test Rule

```shell
POST /ruletest/{id}/start
```

Start the trial run rule, WebSocket will be able to receive the data output after the rule runs.

## Delete the Test Rule

```shell
DELETE /ruletest/{id}
```

Delete the trial run rule, WebSocket will stop the service.
