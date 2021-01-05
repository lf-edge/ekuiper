# Streams management

The Kuiper stream command line tools allows you to manage the streams, such as create, describe, show and drop stream definitions.

## create a stream

The command is used for creating a stream. For more detailed information of stream definition, please refer to [streams](../sqls/streams.md).

```shell
create stream $stream_name $stream_def | create stream -f $stream_def_file
```

- Specify the stream definition in command line.

Sample:

```shell
# bin/kuiper create stream my_stream '(id bigint, name string, score float) WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");'
stream my_stream created
```

The command create a stream named ``my_stream``. 

- Specify the stream definition in file. If the stream is complex, or the stream is already wrote in text files with well organized formats, you can just specify the stream definition through ``-f`` option.

Sample:

```shell
# bin/kuiper create stream -f /tmp/my_stream.txt
stream my_stream created
```

Below is the contents of ``my_stream.txt``.

```json
my_stream(id bigint, name string, score float)
    WITH ( datasource = "topic/temperature", FORMAT = "json", KEY = "id");
```

## show streams

The command is used for displaying all of streams defined in the server.

```shell
show streams
```

Sample:

```shell
# bin/kuiper show streams
my_stream
```

## describe a stream

The command is used for print the detailed definition of stream.

```shell
describe stream $stream_name
```

Sample:

```shell
# bin/kuiper describe stream my_stream
Fields
--------------------------------------------------------------------------------
id	bigint
name	string
score	float

FORMAT: json
KEY: id
DATASOURCE: topic/temperature
```

## drop a stream

The command is used for drop the stream definition.

```shell
drop stream $stream_name
```

Sample:

```shell
# bin/kuiper drop stream my_stream
stream my_stream dropped
```

## query against streams
The command is used for querying data from stream.  
```
query
```

Sample:

```shell
# bin/kuiper query
kuiper > 
```

After typing ``query`` sub-command, it prompts ``kuiper > ``, then type SQLs (see [Kuiper SQL reference](../sqls/overview.md) for how to use Kuiper SQL) in the command prompt and press enter. 

The results will be print in the console.

```shell
kuiper > SELECT * FROM my_stream WHERE id > 10;
[{"...":"..." ....}]
...
```
- Press ``CTRL + C`` to stop the query; 

- If no SQL are type, you can type ``quit`` or ``exit`` to quit the ``kuiper`` prompt console.

