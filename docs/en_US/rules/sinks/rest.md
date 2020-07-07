# REST action

The action is used for publish output message into a RESTful API.

| Property name     | Optional | Description                                                  |
| ----------------- | -------- | ------------------------------------------------------------ |
| method            | true    | The http method for the RESTful API. It is a case insensitive string whose value is among "get", "post", "put", "patch", "delete" and "head". The default value is "get". |
| url             | false    | The RESTful API endpoint, such as ``https://www.example.com/api/dummy``                  |
| bodyType          | true     | The type of the body. Currently, these types are supported: "none", "json", "text", "html", "xml", "javascript" and "form". For "get" and "head", no body is required so the default value is "none". For other http methods, the default value is "json" For "html", "xml" and "javascript", the dateTemplate must be carefully set up to make sure the format is correct. |
| timeout   | true     | The timeout (milliseconds) for a http request, defaults to 5000 ms |
| headers            | true     | The additional headers to be set for the http request. |
| debugResp | true | Control if print the information into the console. If set it to `true`, then print response; If set to `false`, then skip print log. The default is `false`. |
| insecureSkipVerify | true | Control if to skip the certification verification. If it is set to `true`, then skip certification verification; Otherwise, verify the certification. The default value is `true`. |

REST service usually requires a specific data format. That can be imposed by the common sink property `dataTemplate`. Please check the [data template](../overview.md#data-template). Below is a sample configuration for connecting to Edgex Foundry core command. The dataTemplate ``{{.key}}`` means it will print out the value of key, that is result[key]. So the template here is to select only field ``key`` in the result and change the field name to ``newKey``. `sendSingle` is another common property. Set to true means that if the result is an array, each element will be sent individually.
```json
    {
      "rest": {
        "url": "http://127.0.0.1:48082/api/v1/device/cc622d99-f835-4e94-b5cb-b1eff8699dc4/command/51fce08a-ae19-4bce-b431-b9f363bba705",       
        "method": "post",
        "dataTemplate": "\"newKey\":\"{{.key}}\"",
        "sendSingle": true
      }
    }
```