# 函数

Kuiper 具有许多内置函数，可以对数据执行计算。

## 聚合函数
聚合函数对一组值执行计算并返回单个值。聚合函数只能用作以下表达式：
* select 语句的 select 列表（子查询或外部查询）。
* HAVING 子句。

| 函数 | 示例 | 说明             |
| -------- | --------- | ---------------- |
| avg      | avg(col1) | 组中的平均值。空值不参与计算。     |
| count    | count(*)  | 组中的项目数。空值不参与计算。    |
| max      | max(col1) | 组中的最大值。空值不参与计算。     |
| min      | min(col1) | 组中的最小值。空值不参与计算。     |
| sum      | sum(col1) | 组中所有值的总和。空值不参与计算。 |

## 数学函数
| 函数 | 示例   | 说明                                  |
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
| power    | power(x, y) | 返回 x 的 y 次方。 |
| rand     | rand()      | 返回一个伪随机数，其均匀分布在0.0和1.0之间。                     |
| round    | round(col1) | 将值四舍五入到最接近的 BIGINT 值。 |
| sign     | sign(col1)  | 返回给定数字的符号。 当参数的符号为正时，将返回1。 当参数的符号为负数时，返回-1。 如果参数为0，则返回0。 |
| sin      | sin(col1)   | 返回弧度数的正弦值。 |
| sinh     | sinh(col1)  | 返回弧度数的双曲正弦值。                                                     |
| sqrt     | sqrt(col1)  | 返回数字的平方根。  |
| tan      | tan(col1)   | 返回以弧度表示的数字的正切值。 |
| tanh     | tanh(col1)  | 返回弧度数的双曲正切值。 |

## 字符串函数

| 函数 | 示例   | 说明                                  |
| -------- | ----------- | ---------------------------------------------- |
| concat   | concat(col1...)  | 连接数组或字符串。 此函数接受任意数量的参数并返回 String 或 Array |
| endswith | endswith(col1, col2) | 返回一个布尔值，该布尔值指示第一个 String参数是否以第二个 String 参数结尾。 |
| format_time| parse_time(col1, format) | 将日期时间格式化为字符串。 |
| indexof  | indexof(col1, col2)  | 返回第二个参数的第一个索引（从0开始），作为第一个参数中的子字符串。 |
| length   | length(col1)| 返回提供的字符串中的字符数。                                                    |
| lower    | lower(col1) | 返回给定 String 的小写版本。                                                       |
| lpad     | lpad(col1, 2) | 返回 String，在左侧用第二个参数指定的空格数填充。 |
| ltrim    | ltrim(col1) | 从提供的字符串中删除所有前导空格（制表符和空格）。                       |
| numbytes | numbytes(col1) | 以提供的字符串的 UTF-8 编码返回字节数。                  |
| regexp_matches| regexp_matches(col1, regex) | 如果字符串（第一个参数）包含正则表达式的匹配项，则返回 true。 |
| regexp_replace| regexp_matches(col1, regex, str) | 将第一个参数中所有出现的第二个参数（正则表达式）替换为第三个参数。                         |
| regexp_substr| regexp_substr(col1, regex) | 在第一个参数中找到第二个参数（regex）的第一个匹配项。 |
| rpad     | rpad(col1, 2) | 返回字符串参数，在右侧填充第二个参数指定的空格数。 |
| rtrim    | rtrim(col1) | 从提供的字符串中删除所有尾随空白（制表符和空格）。                       |
| substring| substring(col1, start, end) |  从提供的 Int 索引（从0开始，包括0）到字符串的结尾，返回提供的String的子字符串。             |
| startswith| startswith(col1, str) | 返回布尔值，是否第一个字符串参数是否以第二个字符串参数开头。 |
| trim      | trim(col1) | 从提供的字符串中删除所有前导和尾随空格（制表符和空格）。        |
| upper     | upper(col1)| 返回给定 String 的大写版本。 |

## 转换函数

| 函数 | 示例     | 说明                                  |
| -------- | ----------- | ---------------------------------------------- |
| cast     | cast(col,  "bigint") | 将值从一种数据类型转换为另一种数据类型。 支持的类型包括：bigint，float，string，boolean 和 datetime（现在不支持）。 |
| chr      | chr(col1)   | 返回与给定 Int 参数对应的 ASCII 字符                           |
| encode   | encode(col1, "base64") |使用 encode 函数根据编码方案将负载（可能是非 JSON 数据）编码为其字符串表示形式。目前，只支持"base64"econding 类型。                             |
| trunc    | trunc(dec, int)| 将第一个参数截断为第二个参数指定的小数位数。 如果第二个参数小于零，则将其设置为零。 如果第二个参数大于34，则将其设置为34。从结果中去除尾随零。 |

## 哈希函数
| 函数   | 示例         | 说明         |
| ------ | ------------ | ------------ |
| md5    | md5(col1)    | 参数的哈希值 |
| sha1   | sha1(col1)   | 参数的哈希值 |
| sha256 | sha256(col1) | 参数的哈希值 |
| sha384 | sha384(col1) | 参数的哈希值 |
| sha512 | sha512(col1) | 参数的哈希值 |

## JSON 函数
| 函数 | 示例   | 说明                                |
| -------- | ----------- | ---------------------------------------------- |
| json_path_exists      | json_path_exists(col1, "$.name")   | 检查 JSON 路径是否返回指定JSON 值的任何项目。 返回布尔值。 |
| json_path_query     | json_path_query(col1, "$.name")  | 获取 JSON 路径返回的指定 JSON值的所有项目。 |
| json_path_query_first  | json_path_query_first(col1, "$.name")| 获取 JSON 路径返回的指定 JSON值的第一项。 |

**请参阅 [json 路径函数](../json_expr.md#json-path-functions) 了解如何编写json路径。**

## 其它函数
| 函数    | 示例         | 说明                                                         |
| ------- | ------------ | ------------------------------------------------------------ |
| isNull  | isNull(col1) | 如果参数为空值，则返回 true。                                |
| newuuid | newuuid()    | 返回一个随机的16字节 UUID。                                  |
| tstamp  | tstamp()     | 返回当前时间戳，以1970年1月1日星期四00:00:00协调世界时（UTC）为单位。 |
| mqtt    | mqtt(topic)  | 返回指定键的 MQTT 元数据。 当前支持的键包括<br />-topic：返回消息的主题。 如果有多个流源，则在参数中指定源名称。 如 `mqtt(src1.topic)`<br />- messageid：返回消息的消息ID。 如果有多个流源，则在参数中指定源名称。 如 `mqtt(src2.messageid)` |
| meta    | meta(topic)  | 返回指定键的元数据。 键可能是：<br/>-如果 from 子句中只有一个来源，则为独立键，例如`meta(device)`<br />-用于指定流的合格键，例如 `meta(src1.device)` <br />-用于多级元数据的带有箭头的键，例如 `meta(src1.reading->device->name)`。这里假定读取是地图结构元数据。 |
