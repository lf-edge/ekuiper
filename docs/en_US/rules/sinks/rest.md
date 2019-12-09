# REST action

The action is used for publish output message into a RESTful API.

| Property name     | Optional | Description                                                  |
| ----------------- | -------- | ------------------------------------------------------------ |
| method            | true    | The http method for the RESTful API. It is a case insensitive string whose value is among "get", "post", "put", "patch", "delete" and "head". The default value is "get". |
| url             | false    | The RESTful API endpoint, such as ``https://www.example.com/api/dummy``                  |
| bodyType          | true     | The type of the body. Currently, 3 types are supported: "none", "raw" and "form". For "get" and "head", no body is required so the default value is "none". For other http methods, the default value is "raw". |
| timeout   | true     | The timeout (milliseconds) for a http request, defaults to 5000 ms |
| headers            | true     | The additional headers to be set for the http request. |
| sendSingle        | true     | The output messages are received as an array. This is indicate whether to send the results one by one. If false, the output message will be ``{"result":"${the string of received message}"}``. For example, ``{"result":"[{\"count\":30},"\"count\":20}]"}``. Otherwise, the result message will be sent one by one with the actual field name. For the same example as above, it will send ``{"count":30}``, then send ``{"count":20}`` to the RESTful endpoint.Default to false. |

Below is sample configuration for connecting to Edgex Foundry core command.
```json
    {
      "rest": {
        "url": "http://127.0.0.1:48082/api/v1/device/cc622d99-f835-4e94-b5cb-b1eff8699dc4/command/51fce08a-ae19-4bce-b431-b9f363bba705",       
        "method": "post",
        "sendSingle": true
      }
    }
```