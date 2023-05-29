# 转换函数

转换函数采用各种方式对数据进行转换，例如转换类型，使用 base64 编码和压缩。

## CAST

```
cast(col,  "bigint")
``
```

将值从一种数据类型转换为另一种数据类型。 支持的类型包括：bigint，float，string，boolean，bytea 和 datetime。

### 转换为 datetime 类型

使用 cast 函数转换到 datetime 类型时，转换规则如下：

1. 如果参数为 datetime 类型，则直接返回原值。
2. 如果参数为 bigint 或者 float 类型，则其数值会作为自 1970年1月1日0时起至今的毫秒值而转换为 datetime 类型。
3. 如果参数为 string 类型，则会用默认格式 `"2006-01-02T15:04:05.000Z07:00"`  将其转换为 datetime类型。
4. 其他类型的参数均不支持转换。

## CHR

```
chr(col)
```

返回与给定 Int 参数对应的 ASCII 字符

## ENCODE

```
encode(col, "base64")
```

根据编码方案将数据编码为其字符串表示形式。目前，只支持"base64" 编码类型。

## DECODE

```
decode(col, "base64")
```

解码输入字符串。目前，只支持"base64" 类型。

## TRUNC

```
trunc(dec, int)
```

将第一个参数截断为第二个参数指定的小数位数。 如果第二个参数小于零，则将其设置为零。 如果第二个参数大于34，则将其设置为34。从结果中去除尾随零。

## COMPRESS

```
compress(input, "zlib")
```

压缩输入的字符串或二进制值。目前支持 'zlib', 'gzip', 'flate' 和 'zstd' 压缩算法。

## DECOMPRESS

```
decompress(input, "zlib")
```

解压缩输入的字符串或二进制值。目前支持 'zlib', 'gzip', 'flate' 和 'zstd' 压缩算法。