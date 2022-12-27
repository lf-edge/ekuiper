# Rules

Each rule represents a computing job to run in eKuiper. It defines the continuous streaming data source as the input, the computing logic and the result actions as the output.

## Rule Lifecycle

Currently, eKuiper only supports stream processing rule, which means that at lease one of the rule source must be a continuous stream. Thus, the rule will run continuously once started and only stopped if the user send stop command explicitly. The rule may stop abnormally for errors or the eKuiper instance exits.

## Rules Relationship

It is common to run multiple rules simultaneously. As eKuiper is a single instance process, the rules are running in the same memory space. However, there are separated in the runtime and the error in one rule should not affect others. Regarding workload, all rules share the same hardware resource. Each rule can specify the operator buffer to limit the processing rate to avoid taking all resources.

## Rule Pipeline

Multiple rules can form a processing pipeline by specifying a joint point in sink/source. For example, the first rule produce the result to a topic in memory sink and the other rule subscribe to that topic in its memory source. Besides the pair of memory sink/source, users can also use mqtt or other sink/source pair to connect rules.

## More Readings

- [Rule Reference](../guide/rules/overview.md)