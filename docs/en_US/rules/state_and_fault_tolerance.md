## State

Kuiper supports stateful rule stream. There are two kinds of states in Kuiper:
1. Internal state for window operation and rewindable source
2. User state exposed to extensions with stream context, check [state storage](../extension/overview.md#state-storage).

## Fault Tolerance

By default, all the states reside in memory only which means that if the stream exits abnormally, the states will disappear.

In order to make state fault tolerance, Kuipler need to checkpoint the state into persistent storage which will allow a recovery after failure.

### Enable Checkpointing

Set the rule option qos to 1 or 2 will enable the checkpointing. Configure the checkpoint interval by setting the checkpointInterval option.

When things go wrong in a stream processing application, it is possible to have either lost, or duplicated results. For the 3 options of qos, the behavior will be:

1. At-most-once(0): Kuiper makes no effort to recover from failures
2. At-least-once(1): Nothing is lost, but you may experience duplicated results
3. Exactly-once(2): Nothing is lost or duplicated 

Given that Kuiper recovers from faults by rewinding and replaying the source data streams, when the ideal situation is described as exactly once does not mean that every event will be processed exactly once. Instead, it means that every event will affect the state being managed by Kuiper exactly once.

If you don’t need "exactly once", you can gain some performance by configuring Kuiper to use AT_LEAST_ONCE.

### Exactly Once End to End

#### Source consideration

To have an end to end qos of the stream, the source must be rewindable. That means after recovery, the source can be reverted to the checkpointed offset and resend data from that so that the whole stream can be replayed from the last failure.

For extended source, the user must implement the api.Rewindable interface as well as the default api.Source interface. Kuiper will handle the rewind internally.

```go
type Rewindable interface {
	GetOffset() (interface{}, error)
	Rewind(offset interface{}) error
}
```

#### Sink consideration

We cannot guarantee the sink to receive a data exactly once. If failures happen during the period of checkpointing, some states which have sent to the sink may not be checkpointed. And those states will be replayed as they are not restored because of not being checkpointed. In this case, the sink may receive them more than once. 

To implement exactly-once, the user will have to implement deduplication tailored to fit the various sinking system.