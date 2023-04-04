# File Sink

The sink saves the analysis result to a specified file. The [file source](../../sources/builtin/file.md) is the opposite connector that can read the file sink’s output.

## Properties

| Property name | Optional | Description                                                                                                                                                    |
|---------------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path          | false    | The file path for saving the result, such as `/tmp/result.txt`                                                                                                 |
| interval      | true     | The time interval (ms) for flushing the analysis result into the file. The default value is 1000, which means write the analysis result with every one second. |
| fileType      | true     | The type of the file, could be json, csv or lines. Default value is lines. Please check [file types](#file-types) for detail.                                  |
| hasHeader     | true     | Whether to produce the header line. Currently, it is only effective for csv file type.                                                                         |
| delimiter     | true     | The delimiter character, usually apply for the header. Only effective for csv file type.                                                                       |
Other common sink properties are supported. Please refer to the [sink common properties](../overview.md#common-properties) for more information.
Among them, the `format` property is used to define the format of the data in the file. Some file types can only work with specific format. Please check [file types](#file-types) for detail.

### File Types

The file sink can write data into different file types, such as:

- lines: This is the default type. It writes line-separated files that can be decoded by the format parameter in the stream definition. For example, to write line-separated JSON strings, set the file type to lines and the format to json.
- json: This type writes standard JSON array format files. For an example, see [here](https://github.com/lf-edge/ekuiper/tree/master/internal/topo/source/test/test.json). To use this file type, set the format to json.
- csv: This type writes comma-separated csv files. You can also use custom separators. To use this file type, set the format to delimited.

## Sample usage

Below is a sample for selecting temperature greater than 50 degree, and save the result into file `/tmp/result.txt` with every 5 seconds.

```json
{
  "sql": "SELECT * from demo where temperature>50",
  "actions": [
    {
      "file": {
        "path": "/tmp/result.txt",
        "interval": 5000,
        "fileType": "lines",
        "format": "json"
      }
    }
  ]
}
```

