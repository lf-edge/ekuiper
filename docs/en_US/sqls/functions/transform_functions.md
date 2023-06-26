# Transform Functions

Transform functions manipulate data in various ways like convert types, encode with base 64 and compression.

## CAST

```text
cast(col, dataType)
```

Converts a value from one data type to another. The supported types include: bigint, float, string, boolean, bytea and
datetime.

### Cast to datetime

When casting to a datetime type, the supported column type and casting rule are:

1. If a column is datetime type, just return the value.
2. If column is bigint or float type, the number will be treated as the milliseconds elapsed since January 1, 1970 00:
   00:00 UTC and converted.
3. If column is string, it will be parsed to datetime with the default format: `"2006-01-02T15:04:05.000Z07:00"`.
4. Other types are not supported.

## ENCODE

```text
encode(col, encodeType)
```

Use the encode function to encode the payload, which potentially might be non-JSON data, into its string representation
based on the encoding scheme. Currently, only "base64" encoding type is supported.

## DECODE

```text
decode(col, encodeType)
```

Decode the input string with specified decoding method. Currently, only "base64" encoding type is supported.

## COMPRESS

```text
compress(input, method)
```

Compress the input string or binary value with a compression method. Currently, 'zlib', 'gzip', 'flate' and 'zstd'
method are supported.

## DECOMPRESS

```text
decompress(input, method)
```

Decompress the input string or binary value with a compression method. Currently, 'zlib', 'gzip', 'flate' and 'zstd'
method are supported.

## TRUNC

```text
trunc(dec, int)
```

Truncates the first argument to the number of Decimal places specified by the second argument. If the second argument is
less than zero, it is set to zero. If the second argument is greater than 34, it is set to 34. Trailing zeroes are
stripped from the result.

## CHR

```text
chr(col)
```

Returns the ASCII character that corresponds to the given Int argument.

## HEX2DEC

```text
hex2dec(col)
```

Returns the decimal value of the given hexadecimal string. The data type of the parameter needs to be string. If the parameter is `"0x10"` or `"10"`, convert it to `16`.

## DEC2HEX

```text
dec2hex(col)
```

Returns the hexadecimal string of the given Int type decimal, if the parameter is `16`, convert it to `"0x10"`.
