# REST action

The action is used for publish output message into a RESTful API.

| Property name      | Optional | Description                                                                                                                                                                                                                                                                                                                                                                 |
|--------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| method             | true     | The HTTP method for the RESTful API. It is a case insensitive string whose value is among "get", "post", "put", "patch", "delete" and "head". The default value is "get".                                                                                                                                                                                                   |
| url                | false    | The RESTful API endpoint, such as `https://www.example.com/api/dummy`                                                                                                                                                                                                                                                                                                       |
| bodyType           | true     | The type of the body. Currently, these types are supported: "none", "json", "text", "html", "xml", "javascript" and "form". For "get" and "head", no body is required so the default value is "none". For other http methods, the default value is "json" For "html", "xml" and "javascript", the dataTemplate must be carefully set up to make sure the format is correct. |
| timeout            | true     | The timeout (milliseconds) for a HTTP request, defaults to 5000 ms                                                                                                                                                                                                                                                                                                          |
| headers            | true     | The additional headers to be set for the HTTP request.                                                                                                                                                                                                                                                                                                                      |
| debugResp          | true     | Control if print the response information into the console. If set it to `true`, then print response; If set to `false`, then skip print log. The default is `false`.                                                                                                                                                                                                       |
| certificationPath  | true     | The certification path. It can be an absolute path, or a relative path. If it is an relative path, then the base path is where you excuting the `kuiperd` command. For example, if you run `bin/kuiperd` from `/var/kuiper`, then the base path is `/var/kuiper`; If you run `./kuiperd` from `/var/kuiper/bin`, then the base path is `/var/kuiper/bin`.                   |
| privateKeyPath     | true     | The private key path. It can be either absolute path, or relative path, which is similar to use of certificationPath.                                                                                                                                                                                                                                                       |
| rootCaPath         | true     | The location of root ca path. It can be an absolute path, or a relative path, which is similar to use of certificationPath.                                                                                                                                                                                                                                                 |
| insecureSkipVerify | true     | Control if to skip the certification verification. If it is set to `true`, then skip certification verification; Otherwise, verify the certification. The default value is `true`.                                                                                                                                                                                          |
| oAuth              | true     | Define the authentication flow to follow the OAuth style. Other authentication method like apikey can directly set the key to header only, not need to set this configuration. Refer to [OAuth configuration](../../sources/builtin/http_pull.md#OAuth) in httppull source for more information.                                                                            |

Other common sink properties are supported. Please refer to the [sink common properties](../overview.md#common-properties) for more information.

::: v-pre
REST service usually requires a specific data format. That can be imposed by the common sink property `dataTemplate`. Please check the [data template](../data_template.md). Below is a sample configuration for connecting to Edgex Foundry core command. The dataTemplate `{{.key}}` means it will print out the value of key, that is result[key]. So the template here is to select only field `key` in the result and change the field name to `newKey`. `sendSingle` is another common property. Set to true means that if the result is an array, each element will be sent individually.
:::

```json
    {
      "rest": {
        "url": "http://127.0.0.1:59882/api/v1/device/cc622d99-f835-4e94-b5cb-b1eff8699dc4/command/51fce08a-ae19-4bce-b431-b9f363bba705",       
        "method": "post",
        "dataTemplate": "\"newKey\":\"{{.key}}\"",
        "sendSingle": true
      }
    }
```

Example to use oAuth style authentication:

```json
{
  "id": "ruleFollowBack",
  "sql": "SELECT follower FROM followStream",
  "actions": [{
    "rest": {
      "url": "https://com.awebsite/follows",
      "method": "POST",
      "sendSingle": true,
      "bodyType": "json",
      "dataTemplate": "{\"data\":{\"relationships\":{\"follower\":{\"data\":{\"type\":\"users\",\"id\":\"1398589\"}},\"followed\":{\"data\":{\"type\":\"users\",\"id\":\"{{.follower}}\"}}},\"type\":\"follows\"}}",
      "headers": {
        "Content-Type": "application/vnd.api+json",
        "Authorization": "Bearer {{.access_token}}"
      },
      "oAuth": {
        "access": {
          "url": "https://com.awebsite/oauth/token",
          "body": "{\"grant_type\": \"password\",\"username\": \"user@gmail.com\",\"password\": \"mypass\"}",
          "expire": "3600"
        }
      }
    }
  }]
}
```

## Visualization mode

Use visualization create rules SQL and Actions

## Text mode

Use text json create rules SQL and Actions

Example for taosdb rest：
```json
{"id": "rest1",
  "sql": "SELECT tele[0]-\u003eTag00001 AS temperature, tele[0]-\u003eTag00002 AS humidity FROM neuron", 
  "actions": [
    {
      "rest": {
        "bodyType": "text",
        "dataTemplate": "insert into mqtt.kuiper values (now, {{.temperature}}, {{.humidity}})", 
        "debugResp": true,
        "headers": {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="},
        "method": "POST",
        "sendSingle": true,
        "url": "http://xxx.xxx.xxx.xxx:6041/rest/sql"
      }
    }
  ]
}
```

## Configure dynamic properties

There are many scenarios that we need to sink to dynamic url and configurations through REST sink. The properties `method`, `url`,`bodyType` and `headers` support dynamic property through jsonpath syntax. Let's look at an example to modify the previous sample to a dynamic version. Assume we receive data which have metadata like http method and url postfix. We can modify the SQL to fetch these metadata in the result. The rule result will be like:

```json
{
  "method":"post",
  "url":"http://xxx.xxx.xxx.xxx:6041/rest/sql",
  "temperature": 20,
  "humidity": 80
}
```

Then in the action, we set the `method` and `url` to be the value of the result by using data template syntax as below:

```json
{"id": "rest2",
  "sql": "SELECT tele[0]->Tag00001 AS temperature, tele[0]->Tag00002 AS humidity, method, concat(\"http://xxx.xxx.xxx.xxx:6041/rest/sql\", urlPostfix) as url FROM neuron", 
  "actions": [
    {
      "rest": {
        "bodyType": "text",
        "dataTemplate": "insert into mqtt.kuiper values (now, {{.temperature}}, {{.humidity}})", 
        "debugResp": true,
        "headers": {"Authorization": "Basic cm9vdDp0YW9zZGF0YQ=="},
        "method": "{{.method}}",
        "sendSingle": true,
        "url": "{{.url}}"
      }
    }
  ]
}
```
