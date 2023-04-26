# File Sink

The sink saves the analysis result to a specified file. The [file source](../../sources/builtin/file.md) is the opposite
connector that can read the file sink’s output.

## Properties

| Property name         | Optional | Description                                                                                                                                                                                                                                                        |
|-----------------------|----------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| path                  | false    | The file path for saving the result, such as `/tmp/result.txt`. Support to use template for dynamic file name, please check [dynamic properties](../overview.md#dynamic-properties) for detail.                                                                    |
| fileType              | true     | The type of the file, could be json, csv or lines. Default value is lines. Please check [file types](#file-types) for detail.                                                                                                                                      |
| hasHeader             | true     | Whether to produce the header line. Currently, it is only effective for csv file type. Deduce the header from the first data and sort the keys alphabetically.                                                                                                     |
| rollingInterval       | true     | One of the property to set the [rolling strategy](#rolling-strategy). The minimum time interval in millisecond to roll to a new file. The frequency at which this is checked is controlled by the checkInterval.                                                   |
| checkInterval         | true     | One of the property to set the [rolling strategy](#rolling-strategy). The interval in millisecond for checking time based rolling policies. This controls the frequency to check whether a part file should rollover.                                              |
| rollingCount          | true     | One of the property to set the [rolling strategy](#rolling-strategy). The maximum message counts in a file before rollover.                                                                                                                                        |
| rollingNamePattern    | true     | One of the property to set the [rolling strategy](#rolling-strategy). Define how to named the rolling files by specifying where to put the timestamp during file creation. The value could be "prefix", "suffix" or "none".                                        |
| interval (deprecated) | true     | This property is deprecated since 1.10 and will be removed later, please use checkInterval instead. The time interval (ms) for flushing the analysis result into the file. The default value is 1000, which means write the analysis result with every one second. |
| compression           | true     | Compress the payload with the specified compression method. Support  `gzip`, `zstd` method now.                                                                                                                                                                    |

Other common sink properties are supported. Please refer to
the [sink common properties](../overview.md#common-properties) for more information.
Among them, the `format` property is used to define the format of the data in the file. Some file types can only work
with specific format. Please check [file types](#file-types) for detail.

### File Types

The file sink can write data into different file types, such as:

- lines: This is the default type. It writes line-separated files that can be decoded by the format parameter in the
  stream definition. For example, to write line-separated JSON strings, set the file type to lines and the format to
  json.
- json: This type writes standard JSON array format files. For an example,
  see [here](https://github.com/lf-edge/ekuiper/tree/master/internal/topo/source/test/test.json). To use this file type,
  set the format to json.
- csv: This type writes comma-separated csv files. You can also use custom separators. To use this file type, set the
  format to delimited.

### Rolling Strategy

The file sink supports rolling strategy to control the file size and the number of files. The rolling strategy is
controlled by the following properties: rollingInterval, checkInterval, rollingCount and rollingNamePattern.

The file rolling could be based on time or based on message count or both.

1. Time based rolling: The rollingInterval and checkInterval properties are used to control the time based rolling. The
   rollingInterval is the minimum time interval to roll to a new file. The checkInterval is the interval for checking
   time based rolling policies. This controls the frequency to check whether a part file should rollover. For example,
   if checkInterval is 1 hour and rollingInterval is 1 day, then the file sink will check each open file for each hour,
   if the file has opened more than 1 hour, the file will be rolled over. So the actual rolling interval could be bigger
   than rollingInterval property. To use time based rolling, set the rollingInterval property to a positive value and
   set rollingCount to 0. Example combination: rollingInterval=1 day, checkInterval=1 hour, rollingCount=0.
2. Message count based rolling: The rollingCount property is used to control the message count based rolling. The file
   sink will check the message count for each open file, if the message count is greater than rollingCount, the file
   will be rolled over. To use message count based rolling, set the rollingCount property to a positive value and set
   rollingInterval to 0. Example combination: rollingInterval=0, rollingCount=1000.
3. Both time and message count based rolling: The file sink will check both time and message count for each open file,
   if either one is satisfied, the file will be rolled over. To use both time and message count based rolling, set the
   rollingInterval and rollingCount properties to positive values. Example combination: rollingInterval=1 day,
   checkInterval=1 hour, rollingCount=1000.

## Sample usage

Below is a sample for selecting temperature greater than 50 degree, and save the result into file `/tmp/result.txt` with
every 5 seconds.

```json
{
  "sql": "SELECT * from demo where temperature>50",
  "actions": [
    {
      "file": {
        "path": "/tmp/result.txt",
        "fileType": "lines",
        "format": "json"
      }
    }
  ]
}
```

Below is another example to write the result into multiple files based on the `device` field in the payload. Each file
will roll over every 1 hour or have more than 10000 messages. The rolling file name will have a prefix of the creation
timestamp like `1699888888_deviceName.csv`.

```json
{
  "sql": "SELECT * from demo where temperature>50",
  "actions": [
    {
      "file": {
        "path": "{{.device}}.csv",
        "fileType": "csv",
        "format": "delimited",
        "hasHeader": true,
        "delimiter": ",",
        "rollingInterval": 3600000,
        "checkInterval": 600000,
        "rollingCount": 10000,
        "rollingNamePattern": "prefix"
      }
    }
  ]
}
```