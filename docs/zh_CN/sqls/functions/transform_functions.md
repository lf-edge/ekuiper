# 转换函数

转换函数采用各种方式对数据进行转换，例如转换类型，使用 base64 编码和压缩。

## CAST

```text
cast(col,  "bigint")
```

将值从一种数据类型转换为另一种数据类型。支持的类型包括：bigint，float，string，boolean，bytea 和 datetime。

### 转换为 datetime 类型

使用 cast 函数转换到 datetime 类型时，转换规则如下：

1. 如果参数为 datetime 类型，则直接返回原值。
2. 如果参数为 bigint 或者 float 类型，则其数值会作为自 1970年1月1日0时起至今的毫秒值而转换为 datetime 类型。
3. 如果参数为 string 类型，则会尝试自动识别格式并将其转换为 datetime 类型。
   - 支持的时间格式可以参考 `github.com/jinzhu/now` 的 [TimeFormats](https://github.com/jinzhu/now/blob/f067b166b35a996b9ff5a0f610225e1458f23adc/main.go#L17-L27)
4. 其他类型的参数均不支持转换。

## CONVERT_TZ

```text
convert_tz(col, "Asia/Shanghai")
```

将时间数值转换成对应时区的时间。时区参数格式参照 [IANA 时区数据库](https://www.iana.org/time-zones)，默认值为 `UTC`，设置为 `Local` 则使用系统时区。

> 注意：在基于 alpine 的环境里使用该函数，需要确保已经正确安装（`apk add tzdata`）了时区数据。

## TO_SECONDS

```text
to_seconds(col)
```

`to_seconds` 首先将 col 转换为日期时间并将其作为 Unix 时间返回，即自 1970 年 1 月 1 日 UTC 以来经过的秒数。

## CHR

```text
chr(col)
```

返回与给定 Int 参数对应的 ASCII 字符

## ENCODE

```text
encode(col, "base64")
```

根据编码方案将数据编码为其字符串表示形式。目前，只支持 "base64" 编码类型。

## DECODE

```text
decode(col, "base64")
```

解码输入字符串。目前，只支持 "base64" 类型。

## TRUNC

```text
trunc(dec, int)
```

将第一个参数截断为第二个参数指定的小数位数。 如果第二个参数小于零，则将其设置为零。 如果第二个参数大于34，则将其设置为34。从结果中去除尾随零。

## COMPRESS

```text
compress(input, "zlib")
```

压缩输入的字符串或二进制值。目前支持 'zlib', 'gzip', 'flate' 和 'zstd' 压缩算法。

## DECOMPRESS

```text
decompress(input, "zlib")
```

解压缩输入的字符串或二进制值。目前支持 'zlib', 'gzip', 'flate' 和 'zstd' 压缩算法。

## HEX2DEC

```text
hex2dec(col)
```

返回给定16进制字符串的10进制数值, 参数的数据类型需要是 string, 如果参数是 `"0x10"` 或 `"10"`,则将其转换为 `16`。

## DEC2HEX

```text
dec2hex(col)
```

返回给定 Int 类型10进制的16进制字符串,如果参数为 `16`,则将其转换为 `"0x10"`。
