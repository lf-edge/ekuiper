# 字符串函数

字符串函数用于操作字符串数据。

## CONCAT

```text
concat(col1...)
```

连接数组或字符串。 此函数接受任意数量的参数并返回 String 或 Array。

## ENDSWITH

```text
endswith(col1, col2)
```

返回一个布尔值，该布尔值指示第一个 String 参数是否以第二个 String 参数结尾。

## FORMAT_TIME

```text
format_time(col, format)
```

将日期时间格式化为字符串。其中，若参数 col
为兼容类型，则在格式化之前[转换为 datetime 类型](./transform_functions.md#转换为-datetime-类型)
。关于格式字符串，请参考 [时间格式](#时间格式)。

### 时间格式

时间格式为一些特定符号和字母组成的字符串。eKuiper 里支持的符号如下表所示：

| 符号 | 含义       | 示例                                    |
|----|----------|---------------------------------------|
| G  | 公元       | G(AD)                                 |
| Y  | 年        | YYYY(2004), YY(04)                    |
| M  | 月        | M(1), MM(01), MMM(Jan), MMMM(January) |
| d  | 日期       | d(2), dd(02)                          |
| E  | 星期几      | EEE(Mon), EEEE(Monday)                |
| H  | 24小时制的小时 | HH(15)                                |
| h  | 12小时制的小时 | h(2), hh(03)                          |
| a  | AM 或 PM  | a(PM)                                 |
| m  | 分        | m(4), mm(04)                          |
| s  | 秒        | s(5), ss(05)                          |
| S  | 秒的分数     | S(.0), SS(.00), SSS(.000)             |
| z  | 时区名      | z(MST)                                |
| Z  | 4位数的时区   | Z(-0700)                              |
| X  | 时区       | X(-07), XX(-0700), XXX(-07:00)        |

示例:

- YYYY-MM-dd T HH:mm:ss -> 2006-01-02 T 15:04:05
- YYYY/MM/dd HH:mm:ssSSS XXX -> 2006/01/02 15:04:05.000 -07:00

## INDEXOF

```text
indexof(col1, col2)
```

返回第二个参数的第一个索引（从0开始），作为第一个参数中的子字符串。

## LENGTH

```text
length(col)
```

返回提供的字符串中的字符数。

## LOWER

```text
lower(col)
```

返回给定 String 的小写版本。

## LPAD

```text
lpad(col, padNum)
```

返回 String，在左侧用第二个参数指定的空格数填充。请注意，若第二个参数很大，返回的字符串会占据很多内存。尽量避免使用大的长度参数，若参数为变量，可通过
WHERE 语句等方式进行过滤，例如 `SELECT lpad(col, len) from source WHERE len < 999999`。

## LTRIM

```text
ltrim(col)
```

从提供的字符串中删除所有前导空格（制表符和空格）。

## NUMBYTES

```text
numbytes(col)
```

返回提供的字符串中的字节数。

## REGEXP_MATCHES

```text
regexp_matches(col, regex)
```

如果字符串（第一个参数）包含正则表达式的匹配项，则返回 true。

## REGEXP_REPLACE

```text
regexp_replace(col, regex, str)
```

将第一个参数中所有出现的第二个参数（正则表达式）替换为第三个参数。

## REGEXP_SUBSTR

```text
regexp_substr(col, regex)
```

在第一个参数中找到第二个参数（regex）的第一个匹配项。

## RPAD

```text
rpad(col, padNum)
```

返回 String，在右侧填充第二个参数指定的空格数。请注意，若第二个参数很大，返回的字符串会占据很多内存。尽量避免使用大的长度参数，若参数为变量，可通过
WHERE 语句等方式进行过滤，例如 `SELECT rpad(col, len) from source WHERE len < 999999`。

## RTRIM

```text
rtrim(col)
```

从提供的字符串中删除所有尾随空白（制表符和空格）。

## SUBSTRING

```text
substring(col, start, end)
```

返回 String，其中包含从 start 到 end 的子字符串。

## STARTSWITH

```text
startswith(col, str)
```

返回布尔值，是否第一个字符串参数是否以第二个字符串参数开头。

## SPLIT_VALUE

```text
split_value(col, splitter, index)
```

将第一个字符串参数以第二个字符串参数作为分隔符切分，返回切分后的第 index（参数三）个值。

## TRIM

```text
trim(col)
```

从提供的字符串中删除所有前导和尾随空格（制表符和空格）。

## UPPER

```text
upper(col)
```

返回给定 String 的大写版本。

## FORMAT

```text
format(col,D[,locale])
```

将数字X格式化为类似'#######.##'的格式，四舍五入保留D位小数，并将结果作为字符串返回。可选的第三个参数允许指定要用于结果数字小数点的
地区设置。

```sql
SELECT format(12332.1234567, 4,"en_US");
-> '12,332.1235'
```

根据区域的不同，数值的显示格式也大不相同。例如，以下是针对特定区域设置对数字 123456.78 进行格式设置的例子：

| **区域** | **数字格式**   |
|--------|------------|
| en_US  | 123,456.78 |
| de_DE  | 123.456,78 |
| de_CH  | 123'456.78 |

更多地区选项:

| 地区值   | 含义              |
|-------|-----------------|
| ar_AE | 阿拉伯语 - 阿拉伯联合酋长国 |
| ar_BH | 阿拉伯语 - 巴林       |
| ar_DZ | 阿拉伯语 - 阿尔及利亚    |
| ar_EG | 阿拉伯语 - 埃及       |
| ar_IN | 阿拉伯语 - 印度       |
| ar_IQ | 阿拉伯语 - 伊拉克      |
| ar_JO | 阿拉伯语 - 约旦       |
| ar_KW | 阿拉伯语 - 科威特      |
| ar_LB | 阿拉伯语 - 黎巴嫩      |
| ar_LY | 阿拉伯语 - 利比亚      |
| ar_MA | 阿拉伯语 - 摩洛哥      |
| ar_OM | 阿拉伯语 - 阿曼       |
| ar_QA | 阿拉伯语 - 卡塔尔      |
| ar_SA | 阿拉伯语 - 沙特阿拉伯    |
| ar_SD | 阿拉伯语 - 苏丹       |
| ar_SY | 阿拉伯语 - 叙利亚      |
| ar_TN | 阿拉伯语 - 突尼斯      |
| ar_YE | 阿拉伯语 - 也门       |
| be_BY | 白俄罗斯语 - 白俄罗斯    |
| bg_BG | 保加利亚语 - 保加利亚    |
| ca_ES | 加泰罗尼亚语 - 西班牙    |
| cs_CZ | 捷克语 - 捷克共和国     |
| da_DK | 丹麦语 - 丹麦        |
| de_AT | 德语 - 奥地利        |
| de_BE | 德语 - 比利时        |
| de_CH | 德语 - 瑞士         |
| de_DE | 德语 - 德国         |
| de_LU | 德语 - 卢森堡        |
| el_GR | 希腊语 - 希腊        |
| en_AU | 英语 - 澳大利亚       |
| en_CA | 英语 - 加拿大        |
| en_GB | 英语 - 英国         |
| en_IN | 英语 - 印度         |
| en_NZ | 英语 - 新西兰        |
| en_PH | 英语 - 菲律宾        |
| en_US | 英语 - 美国         |
| en_ZA | 英语 - 南非         |
| en_ZW | 英语 - 津巴布韦       |
| es_AR | 西班牙语 - 阿根廷      |
| es_BO | 西班牙语 - 玻利维亚     |
| es_CL | 西班牙语 - 智利       |
| es_CO | 西班牙语 - 哥伦比亚     |
| es_CR | 西班牙语 - 哥斯达黎加    |
| es_DO | 西班牙语 - 多米尼加共和国  |
| es_EC | 西班牙语 - 厄瓜多尔     |
| es_ES | 西班牙语 - 西班牙      |
| es_GT | 西班牙语 - 危地马拉     |
| es_HN | 西班牙语 - 洪都拉斯     |
| es_MX | 西班牙语 - 墨西哥      |
| es_NI | 西班牙语 - 尼加拉瓜     |
| es_PA | 西班牙语 - 巴拿马      |
| es_PE | 西班牙语 - 秘鲁       |
| es_PR | 西班牙语 - 波多黎各     |
| es_PY | 西班牙语 - 巴拉圭      |
| es_SV | 西班牙语 - 萨尔瓦多     |
| es_US | 西班牙语 - 美国       |
| es_UY | 西班牙语 - 乌拉圭      |
| es_VE | 西班牙语 - 委内瑞拉     |
| et_EE | 爱沙尼亚语 - 爱沙尼亚    |
| eu_ES | 巴斯克语 - 西班牙      |
| fi_FI | 芬兰语 - 芬兰        |
| fo_FO | 法罗语 - 法罗群岛      |
| fr_BE | 法语 - 比利时        |
| fr_CA | 法语 - 加拿大        |
| fr_CH | 法语 - 瑞士         |
| fr_FR | 法语 - 法国         |
| fr_LU | 法语 - 卢森堡        |
| gl_ES | 加利西亚语 - 西班牙     |
| gu_IN | 古吉拉特语 - 印度      |
| he_IL | 希伯来语 - 以色列      |
| hi_IN | 印地语 - 印度        |
| hr_HR | 克罗地亚语 - 克罗地亚    |
| hu_HU | 匈牙利语 - 匈牙利      |
| id_ID | 印度尼西亚语 - 印度尼西亚  |
| is_IS | 冰岛语 - 冰岛        |
| it_CH | 意大利语 - 瑞士       |
| it_IT | 意大利语 - 意大利      |
| ja_JP | 日语 - 日本         |
| ko_KR | 朝鲜语 - 韩国        |
| lt_LT | 立陶宛语 - 立陶宛      |
| lv_LV | 拉脱维亚语 - 拉脱维亚    |
| mk_MK | 马其顿语 - 北马其顿     |
| mn_MN | 蒙古语 - 蒙古        |
| ms_MY | 马来语 - 马来西亚      |
| nb_NO | 挪威语（书面） - 挪威    |
| nl_BE | 荷兰语 - 比利时       |
| nl_NL | 荷兰语 - 荷兰        |
| no_NO | 挪威语 - 挪威        |
| pl_PL | 波兰语 - 波兰        |
| pt_BR | 葡萄牙语 - 巴西       |
| pt_PT | 葡萄牙语 - 葡萄牙      |
| rm_CH | 罗曼什语 - 瑞士       |
| ro_RO | 罗马尼亚语 - 罗马尼亚    |
| ru_RU | 俄语 - 俄罗斯        |
| ru_UA | 俄语 - 乌克兰        |
| sk_SK | 斯洛伐克语 - 斯洛伐克    |
| sl_SI | 斯洛文尼亚语 - 斯洛文尼亚  |
| sq_AL | 阿尔巴尼亚语 - 阿尔巴尼亚  |
| sr_RS | 塞尔维亚语 - 塞尔维亚    |
| sv_FI | 瑞典语 - 芬兰        |
| sv_SE | 瑞典语 - 瑞典        |
| ta_IN | 泰米尔语 - 印度       |
| te_IN | 泰卢固语 - 印度       |
| th_TH | 泰语 - 泰国         |
| tr_TR | 土耳其语 - 土耳其      |
| uk_UA | 乌克兰语 - 乌克兰      |
| ur_PK | 乌尔都语 - 巴基斯坦     |
| vi_VN | 越南语 - 越南        |
| zh_CN | 中文 - 中国         |
| zh_HK | 中文 - 香港         |
| zh_TW | 中文 - 台湾         |
