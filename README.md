# Redis Streams Spout for Apache Storm

[![Build Status](https://travis-ci.org/SourceLabOrg/RedisStreams-StormSpout.svg?branch=master)](https://travis-ci.org/SourceLabOrg/RedisStreams-StormSpout)

This project is an [Apache Storm](https://storm.apache.org/) Spout for consuming from [Redis Streams](https://redis.io/topics/streams-intro).

### Features

- Ability to consume from Redis Streams while maintaining state.
- Ability to consume from a single Redis server or a RedisCluster.
- Parallelism supported via unique Consumer Ids.

### Usage & Configuration

Include the dependency in your `POM.xml` file:

```xml
<dependency>
    <groupId>org.sourcelab.storm.spout</groupId>
    <artifactId>redis-stream-spout</artifactId>
    <version>1.1.0</version>
</dependency>
```  

#### Configuration

The spout is configured using the [RedisStreamSpoutConfig](src/main/java/org/sourcelab/storm/spout/redis/RedisStreamSpoutConfig.java) class.  

##### Common Configuration Properties

| Property | Required | Description |
|----------|----------|-------------|
| `Group Name` | Required | The Consumer group name the Spout should use. |
| `Consumer Id Prefix` | Required | A prefix to use for generating unique Consumer Ids within the Consumer Group.  To support multiple parallel consumers, the Spout instance will be appended to the end of this value. |
| `Stream Key` | Required | The Redis key to consume messages from. |
| `Tuple Converter` | Required | Defines how messages are transformed between being consumed from Redis, and being emitted into the topology |
| `Failure Handler` | Required | Defines how the spout handles failed tuples.  See note below. |

#### Example Configuration

```java
    // Create config
    final RedisStreamSpoutConfig.Builder config = RedisStreamSpoutConfig.newBuilder()
        // If you want to connect to a single Redis instance:
        .withServer("localhost", 6759)

        // OR if you want to talk to a RedisCluster:
        .withClusterNode("node1.hostname.com", 6759)
        .withClusterNode("node2.hostname.com", 6759)
        ...
        
        // Consumer Properties
        .withGroupName("StormConsumerGroup")
        .withConsumerIdPrefix("StormConsumer")
        .withStreamKey("RedisStreamKeyName")

        // Tuple Converter instance (see note below)
        .withTupleConverter(..Your TupleConvertor implementation...)

        // Failure Handler instance (see note below)
        .withFailureHandler(new ExponentialBackoffFailureHandler(...));
        

    // Create Spout
    final ISpout redisStreamSpout = new RedisStreamSpout(config);
```

#### TupleConverter Implementation

In order to convert from the values consumed from RedisStream into Tuple values that can be emitted into the Storm Topology,
an implementation of the [TupleConverter](src/main/java/org/sourcelab/storm/spout/redis/TupleConverter.java) must be defined
and passed to the configuration.

[TestTupleConverter](src/test/java/org/sourcelab/storm/spout/redis/example/TestTupleConverter.java) is provided as an example implementation.

#### FailureHandler Implementations

The [FailureHandler](src/main/java/org/sourcelab/storm/spout/redis/FailureHandler.java) interface defines how the Spout
will handle Failed Tuples.  The following implementations are provided out of the box:

| Implementation | Description |
|----------------|-------------|
| [NoRetryHandler](src/main/java/org/sourcelab/storm/spout/redis/failhandler/NoRetryHandler.java) |  Will never retry failed tuples. |
| [ExponentialBackoffFailureHandler](src/main/java/org/sourcelab/storm/spout/redis/failhandler/ExponentialBackoffFailureHandler.java) | Will attempt to retry failed messages using an exponential backoff strategy. |
| [RetryFailedTuples](src/main/java/org/sourcelab/storm/spout/redis/failhandler/RetryFailedTuples.java) | Rudimentary implementation that can be configured to replay failed tuples for a configured number of attempts. |

#### Example Topology

[ExampleLocalTopology](src/test/java/org/sourcelab/storm/spout/redis/example/ExampleLocalTopology.java) is provided as a working
example running on a Local Storm Topology.

# Contributing
## Releasing
Steps for performing a release:

1. Update release version: `mvn versions:set -DnewVersion=X.Y.Z`
2. Validate and then commit version: `mvn versions:commit`
3. Update CHANGELOG and README files.
4. Merge to master.
5. Deploy to Maven Central: `mvn clean deploy -P release-redis-spout`
7. Create release on Github project.

# Changelog

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

[View Changelog](CHANGELOG.md)
