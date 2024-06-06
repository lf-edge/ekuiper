# Notify when current changes

In IoT scenarios, it is a very common scenario to trigger events when indicators change. This article will use current changes as an example to introduce eKuiper SQL rules.

## background

In this scenario, stream will continuously send the current current data in the form of a stream, as well as the timestamp to which the data belongs. eKuiper rules will accept this data flow and meet the requirements with the defined rules, the data is as follows:

```json
{"current":200,"ts":1}
{"current":400,"ts":2}
{"current":300,"ts":3}
{"current":200,"ts":4}
```

### The current changes from less than 300A to more than 300A

```sql
select current, ts from demo where current > 300 and lag(current) < 300;
```

This rule will record the last data of the current, and then compare it with the current data. Once the conditions are met, the event will be triggered.

```json
{"current":200,"ts":1}
{"current":400,"ts":2} output event
{"current":300,"ts":3}
{"current":200,"ts":4}
```

### The total current exceeds 200A for 10 seconds

```sql
select current from demo group by SLIDINGWINDOW(ss,0,10) over (when current > 200) having min(current) > 200;
```

This rule will open a window when receiving current data above 200A. If the smallest data in the window is also greater than 200A, the requirements are met and the event is output.

```json
{"current":100,"ts":1}
{"current":300,"ts":2}
{"current":300,"ts":3}
{"current":300,"ts":4}
{"current":300,"ts":5}
{"current":300,"ts":6}
{"current":300,"ts":7}
{"current":300,"ts":8}
{"current":300,"ts":9}
{"current":300,"ts":10}
{"current":300,"ts":11} output event
```
