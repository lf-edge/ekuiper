# Extensions

eKuiper provides built-in sources, sinks and functions as the building block for the rule. However, it is impossible to cover all external system for source/sink connection such as user's system with private protocol. Moreover, the built-in function cannot cover all the computation needed for all users. Thus, customized source, sink and functions are needed in many cases. eKuiper provide extension mechanism for users to customize all these three aspects.

## Extension Points

We support 3 extension points:

- Source: add new source type for eKuiper to consume data from. The new extended source can be used in the stream/table definition.
- Sink: add new sink type for eKuiper to produce data to. The new extended sink can be used in the rule actions definition.
- Function: add new function type for eKuiper to process data. The new extended function can be used in the rule SQL.

## Extension Types

We support 3 kinds of extension:

- [Go native plugin](../extension/native/overview.md): extend as a Go plugin. It is the most performant, but has a lot of limitation in development and deployment.
- [Portable plugin](../extension/portable/overview.md) with Go or Python language, and it will support more languages later. It simplifies the development and deployment and has less limitations.
- [External service](../extension/external/external_func.md): wrap existing external REST or rpc services as a eKuiper SQL function by configurations. It is a speedy way to extend by existing services. But it only supports function extension.

## More Readings

- [Extension Reference](../extension/overview.md)
