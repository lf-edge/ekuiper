# Timely Stream Processing

Streaming data is a sequence of data over time, in which time is a intrinsic attribute of the data. In timely stream processing, time plays an important role of the computing. For example, when doing aggregations based on certain time periods (typically called windows), to define the time is significant.

## Notion of time

There are typically two domains of time we care about:

- Event time, which is the time at which events actually occurred. Usually, the event should have a timestamp field to indicate its produced time.
- Processing time, which is the time at which events are observed in the system.

In eKuiper, both notion of time are supported.

## Event time and watermark

A stream processor that supports event time needs a way to measure the progress of event time. For example, a window operator that builds hourly windows needs to be notified when event time has passed beyond the end of an hour, so that the operator can close the window in progress.

The mechanism in eKuiper to measure progress in event time is watermarks. Watermarks flow as part of the data stream and carry a timestamp t. A Watermark(t) declares that event time has reached time t in that stream, meaning that there should be no more elements from the stream with a timestamp t' <= t (i.e. events with timestamps older or equal to the watermark). In eKuiper, watermark is in rule level, meaning when reading data from multiple streams, the watermark will flow for all input streams.
