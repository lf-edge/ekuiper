# Test

In this multiple rules scenario benchmark, we will create a shared stream and multiple rules on that single stream. By default, there will be 300 rules with 500 tps which means 150000 processing happen per second. 

## Run the test

Recommend to set kuiper.yaml ignoreCase to false, then start eKuiper and the mqtt broker.
   
0. Setting variables: open `ruleCreator.go`, modify the const variables to set up the eKuiper url, mqtt broker url and how many rules to create. Then open `pub.go` to set up the tps.
1. Build the test util. In this directory, run `go build -o pub100 .` It will produce an executable `pub100`.
2. Create rules: `./pub100 create` which will create the rules. In eKuiper `data/sqliteKV.db` can be backed up. So in the future, just restore this file to create these rules.
3. Run `./pub100`. Monitor the CPU, memory usage and the mqtt sink topic `demoSink` metric to measure the workload.