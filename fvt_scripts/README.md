
## Run JMeter script in local environment
 - Make sure one MQTT broker is available, and make following changes if MQTT broker is not installed at local
    - Modify ``servers`` to your MQTT broker address in Kuiper configuration file ``$kuiper/etc/mqtt_source.yaml``.
    - Modify ``mqtt.server`` to your MQTT broker address in file ``fvt_scripts/rule1.txt``.
 - Specify the ``base`` property in the JMeter command line.
 - Specify the ``fvt`` property in the JMeter command line, below is an example.
    ```
    bin/jmeter.sh -Dbase="/Users/rockyjin/Downloads/workspace/edge/src/kuiper/_build/kuiper-0.1.1-57-g41ea41b-darwin-x86_64" -Dfvt="/Users/rockyjin/Downloads/workspace/edge/src/kuiper"
    ```