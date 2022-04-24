# What is eKuiper

LF Edge eKuiper is an edge lightweight IoT data analytics and stream processing engine. It is a universal edge computing service or middleware designed for resource constrained edge gateways or devices. 

One goal of eKuiper is to migrate the cloud streaming software frameworks (such as [Apache Spark](https://spark.apache.org)，[Apache Storm](https://storm.apache.org) and [Apache Flink](https://flink.apache.org)) to edge side.  eKuiper references these cloud streaming frameworks, and also considered special requirement of edge analytics, and introduced **rule engine**, which is based on ``Source``, ``SQL (business logic)`` and ``Sink``, rule engine is used for developing streaming applications at edge side.

## Why eKuiper

In the IoT era, companies in manufacturing, oil and gas, and transportation, as well as those architecting smart cities and smart buildings keep producing billions of streaming data. The data are massive, continuous and somehow "useless" if they are not analyzed. It is nearly impossible to deal with the streaming data by the traditional batch processing due to the data amount and latency requirements. Stream processing is leveraged to analyze streaming data, and it is becoming even more important in the edge side because the requirements of latency, data security and saving bandwidth cost.

To address these challenges, eKuiper offers a stream processing engine designed for edge computing with these advantages:

1. Low latency: Streaming process near to device generating data and quickly responds to system based on result.
2. More data security: Keep private data in-house without sending over Internet, reduce risk of data breach.
3. Less bandwidth usage: Less bandwidth usage.
4. Simplicity: Use SQL to compose the business logic.

## Use scenarios

It can be run at various IoT edge use scenarios, some common usage scenarios are listed below:

- Real-time processing of production line data in the IIoT
- Gateway of Connected Vehicle analyze the data from data-bus in real time
- Real-time analysis of urban facility data in smart city scenarios. 

The use cases of eKuiper rules include:

- Data transformation pipeline: Form a pipeline to transform data format and do calculation between external data sources like mqtt to another external data sink/consumer.
- Realtime analytic and aggregation: Do calculation near real time. Aggregate data and calculate within continuous time window.
- Data enrich: Join the streaming data with table to enrich the information.
- Abnormal detect and alert: Continuous detect abnormal data and trigger alert.
- Binary processing: Processing binary data such as image and audio. Typical scenario includes auto image thumbnail and image resizing etc.
- Continuous AI infer: Leverage mainstream AI framework to infer streaming data by pre-trained models.

## Computing components

In eKuiper, a computing job is presented as a [rule](rules.md). The rule defines the streaming data sources as the input, the computing logic by SQL and the sinks/actions as the output. 

Once a rule is defined, it will run continuously. It will keep fetching data from the source, calculate according to the SQL logic and trigger the actions with the result. 

## How to submit a computing job

The eKuiper is a long-running service with multiple computing jobs aka. rules running simultaneously. Users can submit and manage the rules through [REST API](../operation/restapi/overview.md), [CLI](../operation/cli/overview.md) and [management UI](../operation/manager-ui/overview.md).

## Where to deploy

eKuiper is designed to run in edge side either in edge gateway or edge device with more than 128MB memory. There is no harm to run in cloud side. However, eKuiper can only run in single instance mode until now.

## Key Features

- Lightweight and high efficiency: Optimized for resource constraint devices with high throughput processing
- Cross CPU and OS support: X86, ARM and PPC CPU arch; Linux distributions, OpenWrt Linux, macOS and Docker
- Connect to different data source:MQTT, EdgeX, HTTP and file etc
- SQL analytics: ANSI SQL queries for quick IoT data analytics
- Sink to different destination: MQTT, EdgeX, HTTP, log, file and databases etc
- Flexible approach to deploy analytic applications: Text-based rules for business logic implementation and deployment through REST API
- Machine learning: Integrate machine learning algorithms and run against streaming data
- Highly extensible: Python and Go language extension SDK for source, sink and function