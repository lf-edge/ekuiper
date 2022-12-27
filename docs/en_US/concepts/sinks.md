# Sinks

Sinks are used to write data to an external system. Sinks can be used to write control data to trigger an action. Sinks can also be used to write status data and save in an external storage.

In a rule, the sink type are used as an action. A rule can have more than one actions and the differenct actions can be the same sink type.

## Result Encoding

The sink result is a string as always. It will be encoded into json string by default. Users can change the format by setting `dataTemplate` which leverage the go template syntax to format the result into a string. For even detail control of the result format, users can develop a sink extension.

## More Readings

- [Sink Reference](../guide/sinks/overview.md)