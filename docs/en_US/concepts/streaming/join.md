# Join of sources

Currently, join is the only way to merge multiple sources in eKuiper. It requires a way to align multiple sources and trigger the join result.

The supported joins in eKuiper include:

- Join of streams: must do in a window.
- Join of stream and table: the stream will be the trigger of join operation.

The supported join type includes LEFT, RIGHT, FULL & CROSS in eKuiper.