# 函数

Kuiper具有许多内置函数，可以对数据执行计算。

## 聚合函数
聚合函数对一组值执行计算并返回单个值。聚合函数只能用作以下表达式：
* select语句的select列表（子查询或外部查询）。
* HAVING子句。

| Function | Example   | 说明             |
| -------- | --------- | ---------------- |
| avg      | avg(col1) | 组中的平均值。空值不参与计算。     |
| count    | count(*)  | 组中的项目数。空值不参与计算。    |
| max      | max(col1) | 组中的最大值。空值不参与计算。     |
| min      | min(col1) | 组中的最小值。空值不参与计算。     |
| sum      | sum(col1) | 组中所有值的总和。空值不参与计算。 |

## 数学函数
| Function | Example     | Description                                    |
| -------- | ----------- | ---------------------------------------------- |
| abs      | abs(col1)   | 绝对值             |
| acos     | acos(col1)  | 弧度数的反余弦值 |
| asin     | asin(col1)  | 弧度数的反正弦值 |
| atan     | atan(col1)  | 弧度数的反正切值 |
| atan2    | atan2(col1, col2)  | 正x轴与两个自变量中定义的（x，y）点之间的弧度角 |
| bitand   | bitand(col1, col2)  | 对两个Int（-converted）参数的位表示执行按位“与”运算 |
| bitor    | bitor(col1, col2)  | 对两个参数的位表示进行或运算                               |
| bitxor   | bitxor(col1, col2)  | 对两个Int（-converted）参数的位表示执行逐位异或运算 |
| bitnot   | bitnot(col1)| 在Int（-converted）参数的位表示形式上执行按位NOT运算        |
| ceil     | ceil(col1)  | 将值舍入到最接近的BIGINT值。 |
| cos      | cos(col1)   | 返回以弧度为单位的数字的余弦值。 |
| cosh     | cosh(col1)  | 返回弧度数的双曲余弦值。                                                     |
| exp      | exp(col1)   | 返回小数点参数的e。 |
| ln       | ln(col1)    | 返回参数的自然对数。 |
| log      | log(col1)   | 返回参数的以10为底的对数。 |
| mod      | mod(col1, col2)   | 返回第一个参数除以第二个参数的余数。                |
| power    | power(x, y) | Pow returns x**y, the base-x exponential of y. |
| rand     | rand()      | 返回一个伪随机数，其均匀分布在0.0和1.0之间。                     |
| round    | round(col1) | 将值四舍五入到最接近的BIGINT值。 |
| sign     | sign(col1)  | 返回给定数字的符号。 当参数的符号为正时，将返回1。 当参数的符号为负数时，返回-1。 如果参数为0，则返回0。 |
| sin      | sin(col1)   | 返回弧度数的正弦值。 |
| sinh     | sinh(col1)  | 返回弧度数的双曲正弦值。                                                     |
| sqrt     | sqrt(col1)  | 返回数字的平方根。  |
| tan      | tan(col1)   | 返回以弧度表示的数字的正切值。 |
| tanh     | tanh(col1)  | 返回弧度数的双曲正切值。 |

## 字符串函数

| Function | Example     | Description                                    |
| -------- | ----------- | ---------------------------------------------- |
| concat   | concat(col1...)  | 连接数组或字符串。 此函数接受任意数量的参数并返回String或Array |
| endswith | endswith(col1, col2) | 返回一个布尔值，该布尔值指示第一个String参数是否以第二个String参数结尾。 |
| format_time| parse_time(col1, format) | 将日期时间格式化为字符串。 |
| indexof  | indexof(col1, col2)  | 返回第二个参数的第一个索引（从0开始），作为第一个参数中的子字符串。 |
| length   | length(col1)| 返回提供的字符串中的字符数。                                                    |
| lower    | lower(col1) | 返回给定String的小写版本。                                                         |
| lpad     | lpad(col1, 2) | 返回String参数，在左侧用第二个参数指定的空格数填充。 |
| ltrim    | ltrim(col1) | 从提供的字符串中删除所有前导空格（制表符和空格）。                       |
| numbytes | numbytes(col1) | 以提供的字符串的UTF-8编码返回字节数。                    |
| regexp_matches| regexp_matches(col1, regex) | 如果字符串（第一个参数）包含正则表达式的匹配项，则返回true。 |
| regexp_replace| regexp_matches(col1, regex, str) | 将第一个参数中所有出现的第二个参数（正则表达式）替换为第三个参数。                         |
| regexp_substr| regexp_substr(col1, regex) | 在第一个参数中找到第二个参数（regex）的第一个匹配项。 |
| rpad     | rpad(col1, 2) | 返回字符串参数，在右侧填充第二个参数指定的空格数。 |
| rtrim    | rtrim(col1) | 从提供的字符串中删除所有尾随空白（制表符和空格）。                       |
| substring| substring(col1, start, end) |  从提供的Int索引（从0开始，包括0）到字符串的结尾，返回提供的String的子字符串。               |
| startswith| startswith(col1, str) | 返回布尔值，是否第一个字符串参数是否以第二个字符串参数开头。 |
| trim      | trim(col1) | 从提供的字符串中删除所有前导和尾随空格（制表符和空格）。        |
| upper     | upper(col1)| 返回给定String的大写版本。 |

## 转换函数

| Function | 示例     | Description                                    |
| -------- | ----------- | ---------------------------------------------- |
| cast     | cast(col,  "bigint") | 将值从一种数据类型转换为另一种数据类型。 支持的类型包括：bigint，float，string，boolean和datetime（现在不支持）。 |
| chr      | chr(col1)   | 返回与给定Int参数对应的ASCII字符                               |
| encode   | encode(col1, "base64") |使用encode函数根据编码方案将负载（可能是非JSON数据）编码为其字符串表示形式。目前，只支持“base64”econding类型。                             |
| trunc    | trunc(dec, int)| 将第一个参数截断为第二个参数指定的小数位数。 如果第二个参数小于零，则将其设置为零。 如果第二个参数大于34，则将其设置为34。从结果中去除尾随零。 |

## 哈希函数
| Function | Example      | 说明         |
| -------- | ------------ | ------------ |
| md5      | md5(col1)    | 参数的哈希值 |
| sha1     | sha1(col1)   | 参数的哈希值 |
| sha256   | sha256(col1) | 参数的哈希值 |
| sha384   | sha384(col1) | 参数的哈希值 |
| sha512   | sha512(col1) | 参数的哈希值 |

## JSON Functions
| Function | Example     | Description                                    |
| -------- | ----------- | ---------------------------------------------- |
| json_path_exists      | json_path_exists(col1, "$.name")   | Checks whether JSON path returns any item for the specified JSON value. Return bool value.                   |
| json_path_query     | json_path_query(col1, "$.name")  | Gets all items returned by JSON path for the specified JSON value.              |
| json_path_query_first  | json_path_query_first(col1, "$.name")| Gets the first item returned by JSON path for the specified JSON value.                  |

**Please refer to [json path functions](../json_expr.md#json-path-functions) for how to compose a json path.**  
## 其它函数
| 函数      | 示例         | Description                                                  |
| --------- | ------------ | ------------------------------------------------------------ |
| isNull    | isNull(col1) | 如果参数为空值，则返回true。                                 |
| newuuid   | newuuid()    | 返回一个随机的16字节UUID。                                   |
| timestamp | timestamp()  | 返回当前时间戳，以1970年1月1日星期四00:00:00协调世界时（UTC）为单位。 |
| mqtt      | mqtt(topic)  | Returns the MQTT meta-data of specified key. The current supported keys<br />- topic: return the topic of message.  If there are multiple stream source, then specify the source name in parameter. Such as ``mqtt(src1.topic)``<br />- messageid: return the message id of message. If there are multiple stream source, then specify the source name in parameter. Such as ``mqtt(src2.messageid)`` |
| meta      | meta(topic)  | Returns the meta-data of specified key. The key could be:<br/> - a standalone key if there is only one source in the from clause, such as ``meta(device)``<br />- A qualified key to specify the stream, such as ``meta(src1.device)`` <br />- A key with arrow for multi level meta data, such as ``meta(src1.reading->device->name)`` This assumes reading is a map structure meta data.|
