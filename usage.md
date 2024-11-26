## Getting Started

### Create Conf for dirwatch Source

PUT /metadata/sources/dirwatch/confKeys/watch1

```json
{
    "path": "/Users/yisa/Downloads/Github/emqx/ekuiper/_build/watch",
    "allowedExtension": ["txt"]
}
```

### Create Stream for dirwatch Source

POST /streams

```json
{
    "sql":" CREATE stream watch () WITH (TYPE=\"dirwatch\",CONF_KEY=\"watch1\");"
}
```

### Create Rule for dirwatch Source

POST /rules

```json
{
  "id": "rule1",
  "sql": "SELECT * from watch",
  "actions": [
    {
      "log": {
      }
    }
  ],
  "options": {
    "qos":1,
    "checkpointInterval": "1s"
  }
}
```

### Create File in dir

Create test.txt file in Dir

```txt
123
```

Recv Sink like below:

```json
{"content":"MTIz","filename":"test.txt","modifyTime":1732241987}
```
