# Rules

Each rule represents a computing job to run in eKuiper. It defines the continuous streaming data source as the input, the computing logic and the result actions as the output.

## Rule Lifecycle

eKuiper currently **only supports Streaming Rules**. These types of rules require at least one Continuous Stream as a
data source.

Once a rule is started, it will run continuously until:

1. The user explicitly sends a stop command.
2. The rule abnormally terminates due to an internal error or the eKuiper instance exiting.

**Asynchronous Rule Startup and Status Management**

The rule startup process is **asynchronous**. When a user sends a start command, eKuiper performs necessary static
checks and then asynchronously executes the rule's startup operation. Therefore:

* The command response received by the user only indicates that eKuiper has accepted the startup request and set the
  rule's **Expected Status** to 'started'.
* This does not mean the rule has begun running. Users need to further check the rule's **Runtime Status** to confirm
  that the rule has successfully started and is running.

**Rule Update and Error Rollback**

During the rule update process, eKuiper provides **rollback** support. If the updated rule fails to start, the system
will automatically maintain and continue running the original old rule to ensure service stability.

## Rules Relationship

It is common to run multiple rules simultaneously. As eKuiper is a single instance process, the rules are running in the same memory space. However, there are separated in the runtime and the error in one rule should not affect others. Regarding workload, all rules share the same hardware resource. Each rule can specify the operator buffer to limit the processing rate to avoid taking all resources.

When multiple rules use a **[Shared Stream](../guide/streams/overview.md#share-source-instance-across-rules)**, they
share the upstream source components, including data ingestion and decoding.

In execution, all rules utilizing a shared stream form a single **Directed Acyclic Graph (DAG)** where downstream rules
can be dynamically added or removed.

**Impact of Shared Streams**

Due to this shared structure, rules within the DAG will influence each other. Specifically:

* **Backpressure Propagation:** Backpressure originating from one rule can propagate backward through the shared source
  component.
* **Wider Impact:** This backpressure on the shared stream will then affect the performance and processing of **all**
  other rules connected to that same shared source.

Besides this, the shared source side ignores checkpoint.

## Rule Pipeline

Multiple rules can form a processing pipeline by specifying a joint point in sink/source. For example, the first rule produce the result to a topic in memory sink and the other rule subscribe to that topic in its memory source. Besides the pair of memory sink/source, users can also use mqtt or other sink/source pair to connect rules.

## More Readings

* [Rule Reference](../guide/rules/overview.md)
