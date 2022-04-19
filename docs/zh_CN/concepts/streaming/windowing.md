# Windowing

As streaming data is infinite, it is impossible to process it as a whole. Windowing provides a mechanism to split the unbounded data into a continuous series of bounded data to calculate.

In eKuiper, the built-in windowing supports:

- Time window: window split by time
- Count window: window split by element count

In time window, both processing time and event time are supported.

For all the supported window type, please check [window functions](../../sqls/windows.md).