## 1 Program description and configuration:

### 1.1 Program description:

​    本程序用于监控并处理命令文件夹中的文件。当程序发现命令文件夹下存在新创建的文件或已更新的文件时，程序将加载这些文件并执行文件中的命令，之后程序将处理过的文件名记录在命令文件夹同级目录的 .history 文件中。 .history 的数据格式如下：

```json
[
  {
        "name":"sample.json",	//已处理过文件的文件名
        "loadTime":1594362344 //处理文件时的时间戳
    }]
```

### 1.2 Command file format and meaning:

| Field       | Optional           | Type     | Description           |
| ----------- | ------------------ | -------- | --------------------- |
| commands    | false              | array    | Command set           |
| url         | false              | string   | http request path     |
| method      | false              | string   | http request method   |
| description | True               | string   | Operation description |
| data        | false for creation | json obj | Creation content      |
|             |                    |          |                       |

### 1.3 Configuration file format and meaning:
```yaml
port: 9081  //kuiper port
timeout: 500  //Timeout for executing a command (unit: ms)
intervalTime: 60  //interval of Checking the command folder  (unit: seconds)
ip: "127.0.0.1" //kuiper ip adress
logPath: "./log/kubernetes.log" //Log save path
commandDir: "./sample/" //Command folder path
```
### 1.4 Compile the program:

Execute the command of `go build -o tools/kubernetes/kubernetes tools/kubernetes/main.go` to generate the kubernetes program.

## 2 Example of stream operation

### 2.1 create stream1

```json
{
    "commands":[
        {
            "url":"/streams",
            "description":"create stream1",
            "method":"post",
            "data":{
                "sql":"create stream stream1 (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");"
            }
        }]
}
```

### 2.2 Show stream list
```json
{
    "commands":[
        {
            "url":"/streams",
            "description":"list stream",
            "method":"get"
        }]
}
```

### 2.3 Get stream1

```json
{
    "commands":[
        {
            "url":"/streams/stream1",
            "description":"get stream1",
            "method":"get"
        }]
}
```

### 2.4 Delete stream1

```json
{
    "commands":[
        {
            "url":"/streams/stream1",
            "description":"del stream1",
            "method":"delete"
        }]
}
```

## 3 Example of rule operation

### 3.1 Create rule1

```json
{
    "commands":[
        {
            "url":"/rules",
            "description":"create rule1",
            "method":"post",
            "data":{
                "id":"rule1",
                "sql":"SELECT * FROM stream1",
                "actions":[
                    {
                        "log":{
                        }
                    }]
            }
        }]
}
```

### 3.2 Show rule list

```json
{
    "commands":[
        {
            "url":"/rules",
            "description":"list rule",
            "method":"get"
        }]
}
```

### 3.3 Get rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1",
            "description":"get rule1",
            "method":"get"
        }]
}
```

### 3.4 Delete rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1",
            "description":"del rule1",
            "method":"delete"
        }]
}

```
### 3.5 Stop rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1/stop",
            "description":"stop rule1",
            "method":"post"
        }]
}
```

### 3.6 Start rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1/start",
            "description":"start rule1",
            "method":"post"
        }]
}
```

### 3.7 Restart rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1/restart",
            "description":"restart rule1",
            "method":"post"
        }]
}
```

### 3.8 Show the status of rule1

```json
{
    "commands":[
        {
            "url":"/rules/rule1/status",
            "description":"get rule1 status",
            "method":"get"
        }]
}
```

## 4 Example of multiple command sets:

```json
{
    "commands":[
        {
            "url":"/streams",
            "description":"create stream1",
            "method":"post",
            "data":{
                "sql":"create stream stream1 (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");"
            }
        },
        {
            "url":"/streams",
            "description":"create stream2",
            "method":"post",
            "data":{
                "sql":"create stream stream2 (id bigint, name string, score float) WITH ( datasource = \"topic/temperature\", FORMAT = \"json\", KEY = \"id\");"
            }
        },
        {
            "url":"/streams",
            "description":"list stream",
            "method":"get"
        },
        {
            "url":"/streams/stream1",
            "description":"get stream1",
            "method":"get"
        },
        {
            "url":"/streams/stream2",
            "description":"del stream2",
            "method":"delete"
        },
        {
            "url":"/rules",
            "description":"create rule1",
            "method":"post",
            "data":{
                "id":"rule1",
                "sql":"SELECT * FROM stream1",
                "actions":[
                    {
                        "log":{
                        }
                    }]
            }
        },
        {
            "url":"/rules",
            "description":"create rule2",
            "method":"post",
            "data":{
                "id":"rule2",
                "sql":"SELECT * FROM stream1",
                "actions":[
                    {
                        "log":{
                        }
                    }]
            }
        },
        {
            "url":"/rules",
            "description":"list rule",
            "method":"get"
        },
        {
            "url":"/rules/rule1",
            "description":"get rule1",
            "method":"get"
        },
        {
            "url":"/rules/rule2",
            "description":"del rule2",
            "method":"delete"
        },
        {
            "url":"/rules/rule1/stop",
            "description":"stop rule1",
            "method":"post"
        },
        {
            "url":"/rules/rule1/start",
            "description":"start rule1",
            "method":"post"
        },
        {
            "url":"/rules/rule1/restart",
            "description":"restart rule1",
            "method":"post"
        },
        {
            "url":"/rules/rule1/status",
            "description":"get rule1 status",
            "method":"get"
        }]
}
```
