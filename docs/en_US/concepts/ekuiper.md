# Architecture Design

LF Edge eKuiper is an edge lightweight IoT data analytics and stream processing engine. It is a universal edge computing service or middleware designed for resource constrained edge gateways or devices.

eKuiper is written by Go. The architecture of eKuiper is as below:

![arch](../resources/arch.png)

As a rule engine, users can submit jobs aka. rule through REST API or CLI. eKuiper rule/SQL parser or graph rule parser will parse, plan and optimize a rule into a flow of processors which leverage streaming runtime and storage if needed.

Processors are loosely coupled and communicate asynchronously with Go channels. Benefit from Go concurrency model, the runtime flow of a rule can

- Communicate asynchronously and non-blocking.
- Easily to engage multiple cores in the modern SMP system.
- Potential to scale in processor level.
- Isolate rules from each other.

These helps eKuiper to achieve low latency and high throughput data processing.

## Computing components

In eKuiper, a computing job is presented as a rule. The rule defines the streaming data sources as the input, the computing logic by SQL and the sinks/actions as the output.

Once a rule is defined, it will run continuously. It will keep fetching data from the source, calculate according to the SQL logic and trigger the actions with the result.

Read further about the components' concepts:

- [rule](./rules.md)
- [source](./sources/overview.md)
- [sink](./sinks.md)
