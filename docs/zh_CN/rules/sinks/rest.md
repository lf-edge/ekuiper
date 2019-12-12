# REST action

The action is used for publish output message into a RESTful API.

| Property name     | Optional | Description                                                  |
| ----------------- | -------- | ------------------------------------------------------------ |
| method            | true    | The http method for the RESTful API. It is a case insensitive string whose value is among "get", "post", "put", "patch", "delete" and "head". The default value is "get". |
| url             | false    | The RESTful API endpoint, such as ``https://www.example.com/api/dummy``                  |
| bodyType          | true     | The type of the body. Currently, these types are supported: "none", "json", "text", "html", "xml", "javascript" and "form". For "get" and "head", no body is required so the default value is "none". For other http methods, the default value is "json" For "html", "xml" and "javascript", the dateTemplate must be carefully set up to make sure the format is correct. |
| dataTemplate      | true     | The [golang template](https://golang.org/pkg/html/template) format string to specify the output data format. The input of the template is the sink message which is always an array of map. If no data template is specified, the raw input will be the data. |
| timeout   | true     | The timeout (milliseconds) for a http request, defaults to 5000 ms |
| headers            | true     | The additional headers to be set for the http request. |
| sendSingle        | true     | The output messages are received as an array. This is indicate whether to send the results one by one. If false, the output message will be ``{"result":"${the string of received message}"}``. For example, ``{"result":"[{\"count\":30},"\"count\":20}]"}``. Otherwise, the result message will be sent one by one with the actual field name. For the same example as above, it will send ``{"count":30}``, then send ``{"count":20}`` to the RESTful endpoint.Default to false. |

Below is sample configuration for connecting to Edgex Foundry core command. The dataTemplate ``{{.key}}`` means it will print out the value of key, that is result[key]. So the template here is to select only field ``key`` in the result and change the field name to ``newKey``
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

## Data Template
If sendSingle is true, the data template will execute against a record; Otherwise, it will execute against the whole array of records. Typical data templates are:

For example, we have the sink input as 
```
[]map[string]interface{}{{
    "ab" : "hello1",
},{
    "ab" : "hello2",
}}
```

In sendSingle=true mode:
- Print out the whole record

```
"dataTemplate": `{"content":{{json .}}}`,
```
- Print out the the ab field

```
"dataTemplate": `{"content":{{.ab}}}`,
```

In sendSingle=false mode:
- Print out the whole record array

```
"dataTemplate": `{"content":{{json .}}}`,
```
- Print out the first record
```
"dataTemplate": `{"content":{{json (index . 0)}}}`,
```
- Print out the field ab of the first record

```
"dataTemplate": `{"content":{{index . 0 "ab"}}}`,
```
- Print out field ab of each record in the array to html format
```
"dataTemplate": `<div>results</div><ul>{{range .}}<li>{{.ab}}</li>{{end}}</ul>`,
```