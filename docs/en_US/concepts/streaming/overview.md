# Stream Processing

Streaming data is a sequence of data elements made available over time. Stream processing is the processing of streaming data in motion. Unlike batch processing, streaming data is processed one at a time once it is produced.

## Streaming Characteristic

Stream processing has the below characteristics:

- Unbounded data: streaming data is a type of ever-growing, essentially infinite data set which cannot be operated as a whole. 
- Unbounded data processing: As applying to unbounded data, the stream processing itself is also unbounded. The workload can distribute evenly across time compared to batch processing.
- Low-latency, near real-time: stream processing can process data once it is produced to get the result in a very low latency.

Stream processing unifies applications and analytics. This simplifies the overall infrastructure, because many systems can be built on a common architecture, and also allows a developer to build applications that use analytical results to respond to insights in the data to take action directly.

## Edge Stream Processing

On the edge side, the majority of data are born as continuous streams such as sensor events. With the wide application of IoT, more and more edge computing nodes need to access the cloud network and generate huge amount of data. In order to reduce the communication cost, reduce the data volume of data on the cloud, and at the same time improve the real-time data processing to achieve the purpose of local timely response and also local timely data processing in case of network disconnection, it is necessary to introduce real-time stream processing at the edge.

## Stateful Stream Processing

Stateful stream processing is a subset of stream processing in which the computation maintains contextual state. Some examples of stateful stream processing:

- When aggregating events to calculate sum, count or average values.
- When detecting event changes.
- When searching for a pattern across a series of events.

The state information can be found or managed by:

- [Windows](./windowing.md)
- [State API](../../extension/native/overview.md#state-storage)

