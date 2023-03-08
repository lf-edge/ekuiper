# File Sink

The sink is used for saving analysis result into a specified file.

## Properties

| Property name | Optional | Description                                                                                                                                     |
|---------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| path          | false    | The file path for saving the result, such as `/tmp/result.txt`                                                                                  |
| interval      | true     | The time interval (ms) for writing the analysis result. The default value is 1000, which means write the analysis result with every one second. |

## Sample usage

Below is a sample for selecting temperature great than 50 degree, and save the result into file `/tmp/result.txt` with every 5 seconds.

```json
{
  "sql": "SELECT * from demo where temperature>50",
  "actions": [
    {
      "file": {
        "path": "/tmp/result.txt",
        "interval": 5000
      }
    }
  ]
}
```

