# zkafka

[![License](https://img.shields.io/github/license/zillow/zkafka)](https://github.com/zillow/zkafka/blob/main/LICENSE)
[![GitHub Actions](https://github.com/zillow/zkafka/actions/workflows/go.yml/badge.svg)](https://github.com/zillow/zkafka/actions/workflows/go.yml)
[![Codecov](https://codecov.io/gh/zillow/zkafka/branch/main/graph/badge.svg?token=STRT8T67YP)](https://codecov.io/gh/zillow/zkafka)
[![Go Report Card](https://goreportcard.com/badge/github.com/zillow/zkafka)](https://goreportcard.com/report/github.com/zillow/zkafka)

## Install

`go get -u github.com/zillow/zkafka`

## About

`zkafka` is built to simplify message processing in Kafka. This library aims to minimize boilerplate code, allowing the developer to focus on writing the business logic for each Kafka message. `zkafka` takes care of various responsibilities, including:

1. Reading from the worker's configured topics
2. Managing message offsets reliably - Kafka offset management can be complex, but `zkafka` handles it. Developers only need to write code to process a single message and indicate whether or not it encountered an error.
3. Distributing messages to virtual partitions (details will be explained later)
4. Implementing dead lettering for failed messages
5. Providing inspectable and customizable behavior through lifecycle functions (Callbacks) - Developers can add metrics or logging at specific points in the message processing lifecycle.

`zkafka` provides stateless message processing semantics ( sometimes, called lambda message processing).
This is a churched-up way of saying, "You write code which executes on each message individually (without knowledge of other messages)".
It is purpose-built with this type of usage in mind. Additionally, the worker implementation guarantees at least once processing (Details of how that's achieved are shown in the [Commit Strategy](#commit-strategy) section)

---
**NOTE**
`zkafka` is  built on top of [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go)
which is a CGO module. Therefore, so is `zkafka`. When building with `zkafka`, make sure to set CGO_ENABLED=1.
---

### Features

The following subsections detail some useful features. To make the following sections more accessible, there are runnable examples in `./examples` directory.
The best way to learn is to experiment with the examples. Dive in!

#### Stateless Message Processing

`zkafka` makes stateless message processing easy. All you have to do is write a concrete `processor` implementation and wire it up (shown below).

```go 
type processor interface {
    Process(ctx context.Context, message *zkafka.Message) error
}
```
If you want to skip ahead and see a working processor check out the examples. Specifically `example/worker/main.go`.

The anatomy of that example is described here:

A `zkafka.Client` needs to be created which can connect to the kafka broker. Typically, authentication
information must also be specified at this point (today that would include username/password).

```go 
    client := zkafka.NewClient(zkafka.Config{ BootstrapServers: []string{"localhost:29092"} })
```

Next, this client should be passed to create a `zkafka.WorkFactory` instance.
The factory design, used by this library, adds a little boilerplate but allows default policies to be injected and
proliferated to all instantiated work instances. We find that useful at zillow for transparently injecting the nuts and bolts
of components that are necessary for our solutions to cross-cutting concerns (typically those revolving around telemetry)

```go 
    wf := zkafka.NewWorkFactory(client)
```

Next we create the work instance. This is finally where the dots are beginning to connect.
`zkafka.Work` objects are responsible for continually polling topics (the set of whom is specified in the config object) they've been instructed
to listen to, and execute specified code (defined in the user-controlled `processor` and `lifecycle` functions (not shown here))
```go 
   topicConfig := zkafka.TopicConfig{Topic: "my-topic", GroupdID: "mygroup", ClientID: "myclient"}
   // this implements the interface specified above and will be executed for each read message
   processor := &Processor{}
   work := wf.Create(topicConfig, processor)
```

All that's left now is to kick off the run loop (this will connect to the Kafka broker, create a Kafka consumer group, undergo consumer group assignments, and after the assignment begins polling for messages).
The run loop executes a single reader (Kafka consumer) which reads messages and then fans those messages out to N processors (sized by the virtual partition pool size. Described later).
It's a processing pipeline with a reader at the front, and processors at the back.

The run loop takes two arguments, both responsible for signaling that the run loop should exit.

1. `context.Context` object. When this object is canceled, the internal
   work loop will begin to abruptly shut down. This involves exiting the reader loop and processor loops immediately.

2. signal channel. This channel should be `closed`, and tells zkafka to begin a graceful shutdown.
   Graceful shutdown means the reader stops reading new messages, and the processors attempt to finish their in-flight work.

At Zillow, we deploy to a kubernetes cluster, and use a strategy that uses both
mechanisms. When k8s indicates shutdown is imminent, we close the `shutdown` channel. Graceful
shutdown is time-boxed, and if the deadline is reached, the outer `context` object
is canceled signaling a more aggressive teardown. The below example passes in a nil shutdown signal (which is valid).
That's done for brevity in the readme, production use cases should take advantage (see examples).

```go 
   err = w.Run(context.Background(), nil)
```

#### Hyper Scalability

`zkafka.Work` supports a concept called `virtual partitions`. This extends
the Kafka `partition` concept. Message ordering is guaranteed within a Kafka partition,
and the same holds true for a `virtual partition`. Every `zkafka.Work` object manages a pool
of goroutines called processors (1 by default and controlled by the `zkafka.Speedup(n int)` option).
Each processor reads from a goroutine channel called a `virtual partition`.
When a message is read by the reader, it is assigned to one of the virtual partitions based on `hash(message.Key) % virtual partition count`.
This follows the same mechanism used by Kafka. With this strategy, a message with the same key will be assigned
to the same virtual partition.

This allows for another layer of scalability. To increase throughput and maintain the same
message ordering guarantees, there is no longer a need to increase the Kafka partition count (which can be operationally challenging).
Instead, you can use `zkafka.Speedup()` to increase the virtual partition count.

```shell
// sets up Kafka broker locally
make setup;
// terminal 1. Starts producing messages. To juice up the production rate, remove the time.Sleep() in the producer and turn acks off.
make example-producer
// terminal 2. Starts a worker with speedup=5. 
make example-worker
```

#### Configurable Dead Letter Topics

A `zkafka.Work` instance can be configured to write to a Dead Letter Topic (DLT) when message processing fails.
This can be accomplished with the `zkafka.WithDeadLetterTopic()` option. Or, more conveniently, can be controlled by adding
a non nil value to the `zkafka.ConsumerTopicConfig` `DeadLetterTopic` field. Minimally, the topic name of the (dead letter topic)
must be specified (when specified via configuration, no clientID need be specified, as the encompassing consumer topic configs client id will be used).

```go 
     zkafka.ConsumerTopicConfig{
        ...
       // When DeadLetterTopicConfig is specified a dead letter topic will be configured and written to
       // when a processing error occurs.
       DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
          Topic:    "zkafka-example-deadletter-topic",
       },
    }
```

The above will be written to `zkafka-example-deadletter-topic` in the case of a processing error.


The above-returned error will skip writing to the DLT.

To execute a local example of the following pattern:

```shell
// sets up kafka broker locally
make setup;
// terminal 1. Starts producing messages (1 per second)
make example-producer
// terminal 2. Starts a worker which fails processing and writes to a DLT. Log statements show when messaages
// are written to a DLT
make example-deadletter-worker
```

The returned processor error determines whether a message is written to a dead letter topic. In some situations,
you might not want to route an error to a DLT. An example might be malformed data.

You have control over this behavior by way of the `zkafka.ProcessError`.

```go 
    return zkafka.ProcessError{
       Err:                 err,
       DisableDLTWrite:     true,
    }
```
#### Process Delay Workers

Process Delay Workers can be an important piece of an automated retry policy. A simple example of this would be
2 workers daisy-chained together as follows:

```go 
     workerConfig1 := zkafka.ConsumerTopicConfig{
       ClientID: "svc1",
       GroupID: "grp1",
        Topic: "topicA",
       // When DeadLetterTopicConfig is specified a dead letter topic will be configured and written to
       // when a processing error occurs.
       DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
          Topic:    "topicB",
       },
    }

    workerConfig2 := zkafka.ConsumerTopicConfig{
      ClientID: "svc1",
      GroupID: "grp1",
      Topic: "topicB",
      // When DeadLetterTopicConfig is specified a dead letter topic will be configured and written to
      // when a processing error occurs.
      DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
         Topic:    "topicC",
      },
    }
```

Messages processed by the above worker configuration would:

1.  Worker1 read from `topicA`
2. If message processing fails, write to `topicB` via the DLT configuration
3. Worker2 read from `topicB`
4. If message processing fails, write to `topicC` via the DLT configuration

This creates a retry pipeline. The issue is that worker2, ideally would process on a delay (giving whatever transient error is occurring a chance to resolve).
Luckily, `zkafka` supports such a pattern. By specifying `ProcessDelayMillis` in the config, a worker is created which will delay processing of
a read message until at least the delay duration has been waited.

```go 
    topicConfig := zkafka.ConsumerTopicConfig{
        ... 
       // This value instructs the kafka worker to inspect the message timestamp, and not call the processor call back until
       // at least the process delay duration has passed
       ProcessDelayMillis: &processDelayMillis,
    }
```

The time awaited by the worker varies. If the message is very old (maybe the worker had been stopped previously),
then the worker will detect that the time passed since the message was written > delay. In such a case, it won't delay any further.

To execute a local example of the following pattern:

```shell
// sets up kafka broker locally
make setup;
// terminal 1. Starts producing messages (1 per second)
make example-producer
// terminal 2. Starts delay processor. Prints out the duration since msg.Timestamp. 
// How long the delay is between when the message was written and when the process callback is executed.
make example-delay-worker
```

### Commit Strategy:

A `zkafka.Work`er commit strategy allows for at least once message processing.

There are two quick definitions important to the understanding of the commit strategy:

1. **Commit** - involves communicating with kafka broker and durably persisting offsets on a kafka broker.
2. **Store** - is the action of updating a local store of message offsets which will be persisted during the commit
   action

The `zkafka.Work` instance will store message offsets as message processing concludes. Because the worker manages
storing commits the library sets `enable.auto.offset.store`=false. Additionally, the library offloads actually committing messages
to a background process managed by `librdkafka` (The frequency at which commits are communicated to the broker is controlled by `auto.commit.interval.ms`, default=5s).
Additionally, during rebalance events, explicit commits are executed.

This strategy is based off of [Kafka Docs - Offset Management](https://docs.confluent.io/platform/current/clients/consumer.html#offset-management)
where a strategy of asynchronous/synchronous commits is suggested to reduce duplicate messages.

The above results in the following algorithm:


1. Before message processing is started, an internal heap structure is used to track in-flight messages.
2. After message processing concludes, a heap structure managed by `zkafka` marks the message as complete (regardless of whether processing errored or not).
3. The inflight heap and the work completed heap are compared. Since offsets increase incrementally (by 1), it can be determined whether message processing
   finished out of order. If the inflight heap's lowest offset is the same as the completed, then that message is safe to be **Stored**. This can be done repetitively
   until the inflight heap is empty, or inflight messages haven't yet been marked as complete.

The remaining steps are implicitly handled by `librdkafka`
1. *Commit* messages whose offsets have been stored at configurable intervals (`auto.commit.interval.ms`)
2. *Commit* messages whose offsets have been stored when partitions are revoked
   (this is implicitly handled by `librdkafka`. To see this add debug=cgrp in ConsumerTopicConfig, and there'll be COMMIT logs after a rebalance.
   If doing this experience, set the `auto.commit.interval.ms` to a large value to avoid confusion between the rebalance commit)
3. *Commit* messages whose offsets have been stored on close of reader
   (this is implicitly handled by `librdkafka`. To see this add debug=cgrp in ConsumerTopicConfig, and there'll be COMMIT logs after the client is closed, but before the client is destroyed)

Errors returned on processing are still stored. This avoids issues due to poison pill messages (messages that will
never be able to be processed without error)
as well as transient errors blocking future message processing. Use dead lettering to sequester these failed messages or Use `WithOnDone()` option to register callback for
special processing of these messages.


### SchemaRegistry Support:

There is limited support for schema registry in zkafka. A schemaID can be hardcoded via configuration. No
communication is done with schema registry, but some primitive checks can be conducted if a schemaID is specified via
configuration.

Below is a breakdown of schema registry interactions into two subcategories. One is `Raw Handling` where the configurable
foramtter is bypassed entirely in favor of operating with the value byte arrays directly. The other is `Native Support` which
attempts to create confluent compatible serializations, without communicating with schema registry directly.

#### Producers

##### Raw Handling

For a producer, this would involve using the `kafka.Writer.WriteRaw()` method which takes in a byte array directly.

##### Native Support

Producers will include the schemaID in messages written to kafka (without any further verification).

#### Consumers

##### Raw Handling

For a consumer, this would involve accessing the value byte array through the `zkafka.Message.Value()` method.

```go 
type Processor struct{}
func (p Processor) Process(_ context.Context, msg *zkafka.Message) error {
   e := MyEvent{}
   // The Decode method uses the configured formatter and the configured schema registry ID.
   //err := msg.Decode(&e)
   // For schema registry, however, it might be better to bypass the configurable formatters, and deserialize the data by accessing the byte array directly
   // The below function is a hypothetical function that inspects the data in in the kafka message's value and communicates with schema registry for verification
   myUnmarshallFunction(msg.Value(), &e)
   ...
}
```

##### Native Support
Consumers will verify that the message they're consuming has the schemaID specified in the configuration
(if it's specified). Be careful here, as backwards compatible schema evolutions would be treated as an error condition
as the new schemaID wouldn't match what's in the configuration.

### Consumer/Producer Configuration

See for description of configuration options and their defaults:

1. [Librdkafka Configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)
2. [Consumer Configuration](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)
3. [Producer Configurations](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)

These are primarily specified through the TopicConfig structs (`ProducerTopicConfig` and `ConsumerTopicConfig`).
TopicConfigs includes strongly typed fields that translate
to librdconfig values. To see translation see `config.go`. An escape hatch is provided for ad hoc config properties via
the AdditionalProperties map. Here config values that don't have a strongly typed version in TopicConfig may be
specified. Not all specified config values will work (for example `enable.auto.commit=false` would not work with this
client because that value is explicitly set to true after reading of the AdditionalProperties map).

```go
deliveryTimeoutMS := 100
enableIdempotence := false
requiredAcks := "0"

pcfg := ProducerTopicConfig{
   ClientID:            "myclientid",
   Topic: "mytopic",
   DeliveryTimeoutMs:   &deliveryTimeoutMS,
   EnableIdempotence:   &enableIdempotence,
   RequestRequiredAcks: &requiredAcks,
   AdditionalProps: map[string]any{
      "linger.ms":               float64(5),
   },
}

ccfg := ConsumerTopicConfig{
   ClientID: "myclientid2",
   GroupID:  "mygroup",
   Topic: "mytopic",
   AdditionalProps: map[string]any{
      "auto.commit.interval.ms": float32(20),
   },
}
```
