# Spark Kryo Encoder Bug Reproduction

This repository contains minimal examples to reproduce known bugs in Apache Spark 4.0.0's Kryo encoder.

## Overview

- **runFlatMapGroupState**: Reproduces the `FlatMapGroupsWithState` bug described in [SPARK-52819](https://issues.apache.org/jira/browse/SPARK-52819).
- **runAggr**: Reproduces a similar serialization bug in a user-defined aggregation function using Kryo encoder.

## Prerequisites

- Java 17 or higher

## Building the Project

To compile the code and assemble the JAR, run:

```bash
./gradlew clean build
```

## Running the Examples

### FlatMapGroupsWithState Bug

Runs the Java example that demonstrates the `FlatMapGroupsWithState` serialization error:

```bash
./gradlew runFlatMapGroupState
```

### Aggregator (Kryo) Bug

Runs the Scala example that demonstrates the user-defined aggregation function bug with Kryo encoder:

```bash
./gradlew runAggr
```

## Expected Behavior

Both tasks will fail with a serialization exception or produce incorrect results due to the Kryo encoder issue in Spark 4.0.0.

## Sample Logs

### FlatMapGroupsWithState Bug Log

<details>
<summary>Show full FlatMapGroupsWithState log</summary>

```text
25/07/22 11:51:09 INFO SparkContext: Running Spark version 4.0.0
25/07/22 11:51:12 ERROR MicroBatchExecution: Query terminated with error
org.apache.spark.SparkException: Task not serializable
Caused by: java.io.NotSerializableException: org.apache.spark.sql.catalyst.encoders.KryoSerializationCodec$
        at org.apache.spark.serializer.SerializationDebugger$.improveException(SerializationDebugger.scala:43)
        at org.apache.spark.serializer.JavaSerializationStream.writeObject(JavaSerializer.scala:50)
        at org.apache.spark.serializer.JavaSerializerInstance.serialize(JavaSerializer.scala:122)
        at org.apache.spark.util.SparkClosureCleaner$.clean(SparkClosureCleaner.scala:42)
        at org.apache.spark.SparkContext.clean(SparkContext.scala:2839)
        at org.apache.spark.sql.execution.streaming.state.package$StateStoreOps.mapPartitionsWithStateStore(package.scala:67)
        at org.apache.spark.sql.execution.streaming.FlatMapGroupsWithStateExecBase.doExecute(FlatMapGroupsWithStateExec.scala:263)
        at org.apache.spark.sql.execution.streaming.FlatMapGroupsWithStateExec.doExecute(FlatMapGroupsWithStateExec.scala:403)
        at org.apache.spark.sql.execution.SparkPlan.$anonfun$executeRDD$1(SparkPlan.scala:188)
...
Exception in thread "main" org.apache.spark.sql.streaming.StreamingQueryException: Task not serializable
Caused by: org.apache.spark.SparkException: Task not serializable
        at org.apache.spark.util.SparkClosureCleaner$.clean(SparkClosureCleaner.scala:45)
        at org.apache.spark.SparkContext.clean(SparkContext.scala:2839)
        at org.apache.spark.sql.execution.streaming.StreamExecution.org$apache$spark$sql$execution$streaming$StreamExecution$$runStream(StreamExecution.scala:372)
        at org.apache.spark.sql.execution.streaming.StreamExecution$$anon$1.run(StreamExecution.scala:226)
> Task :runFlatMapGroupState FAILED
```  
</details>

### Aggregator (Kryo) Bug Log

<details>
<summary>Show full Aggregator (Kryo) log</summary>

```text
25/07/22 11:51:29 INFO Executor: Starting executor ID driver
25/07/22 11:51:30 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Job aborted due to stage failure: Task not serializable
Caused by: java.io.NotSerializableException: org.apache.spark.sql.catalyst.encoders.KryoSerializationCodec$
Serialization stack:
        - object not serializable (class: org.apache.spark.sql.catalyst.encoders.KryoSerializationCodec$, value: <function0>)
        - field (class: org.apache.spark.sql.catalyst.encoders.AgnosticEncoders$TransformingEncoder, name: codecProvider)
        - object (class: org.apache.spark.sql.catalyst.encoders.AgnosticEncoders$TransformingEncoder)
        - field (class: org.apache.spark.sql.catalyst.encoders.ExpressionEncoder, name: encoder)
        - object (class: org.apache.spark.sql.catalyst.encoders.ExpressionEncoder)
        - field (class: org.apache.spark.sql.execution.aggregate.ScalaAggregator, name: bufferEncoder)
        - object (class: org.apache.spark.sql.execution.aggregate.ScalaAggregator)
...
        at org.apache.spark.scheduler.DAGScheduler.submitMissingTasks(DAGScheduler.scala:1664)
> Task :runAggr FAILED
```  
</details>
