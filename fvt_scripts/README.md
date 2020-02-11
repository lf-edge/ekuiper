
## Run JMeter script in local environment
 - Specify the ``base`` property in the JMeter command line, below is an example.
    ```
    bin/jmeter.sh -Dbase="/Users/rockyjin/Downloads/workspace/edge/src/kuiper/_build/kuiper-0.1.1-57-g41ea41b-darwin-x86_64"
    ```
 - Copy ``rule1.txt`` into directory specified in last step
    ```
    cp $kuiper_ws/fvt_scripts/rule1.txt $base
    ```