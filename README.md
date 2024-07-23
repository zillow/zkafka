# zkafka

[![License](https://img.shields.io/github/license/zillow/zkafka)](https://github.com/zillow/zkafka/blob/main/LICENSE)
[![GitHub Actions](https://github.com/zillow/zkafka/actions/workflows/go.yml/badge.svg)](https://github.com/zillow/zkafka/actions/workflows/go.yml)
[![Codecov](https://codecov.io/gh/zillow/zkafka/branch/main/graph/badge.svg?token=STRT8T67YP)](https://codecov.io/gh/zillow/zkafka)
[![Go Report Card](https://goreportcard.com/badge/github.com/zillow/zkafka)](https://goreportcard.com/report/github.com/zillow/zkafka)

## Install

`go get -u github.com/zillow/zkafka`

## About

A library built on top of [confluent-kafka-go](https://github.com/confluentinc/confluent-kafka-go) for reading and writing to kafka. 
It's tailor-made for stateless message processing use cases (a churched up name for writing code which processes each kafka message in isolation).
The library supports at least once message processing. It does so using a commit strategy built off auto commit and manual
offset storage.

---
**NOTE**

confluent-kafka-go is a CGO module, and therefore so is zkafka. When building with zkafka, make sure to set CGO_ENABLED=1.
---

There are two quick definitions important to the understanding of the commit strategy

1. **Commit** - involves communicating with kafka broker and durably persisting offsets on a kafka broker.
2. **Store** - is the action of updating a local store of message offsets which will be persisted during the commit
   action

### Features
- Stateless Message Processing
- Hyper Scalability
- Dead Letter Topics
- Delay Processing

#### Stateless Message Processing

`zkafka` makes stateless message processing easy. Skipping ahead, the code author is responsible for writing a type which implements the
following interface.

```go 
type processor interface {
	Process(ctx context.Context, message *Message) error
}
```

The `Process` method will then be called for each message read by the work implementation.
`example/worker/worker.go` shows the full detailed setup.

The boilerplate setup is described here:

A zkafka.Client needs to be created which can connect to the kafka broker. Typically, authentication
information must also be specified at this point (today that would include username/password)
```go 
	client := zkafka.NewClient(zkafka.Config{ BootstrapServers: []string{"localhost:29092"} })
```

Next this client should be passed to create a `zkafka.WorkFactory` instance.
The factory design adds a little boilerplate, but allows default policies to be injected and then
proliferated to created work instances.

```go 
	wf := zkafka.NewWorkFactory(client)
```

Next we create the work instance. This is finally where the dots are begining to connext.
`zkafka.Work` objects are responsible for continually polling topics they've been instructed
to listen to, and then executing customer specified code (defined in the user controlled `processor`)
```go 
   topicConfig := zkafka.TopicConfig{Topic: "my-topic", GroupdID: "mygroup", ClientID: "myclient"}
   // this implements the interface specified above and will be executed for each read message
   processor := &Processor{}
   work := wf.Create(topicConfig, processor)
```


All that's left now is to kick off the run loop. The runloop takes two arguments,
both responsible for signalling that the run loop should exit.

The first is a context object. When this object is cancelled, the internal
work loop will begin to abruptly shutdown. 

The second is a signal channel. This channel should be `closed`, and tells
zkafka to begin a graceful shutdown. Graceful shutdown means new messages from
the kafka topic won't be read, but ongoing work will be allowed to finish.

At zillow, we deploy to kubernetes cluster, and use a strategy which uses both
mechanisms. When k8s indicates shutdown is imminent, we close the `shutdown` channel. Graceful
shutdown is time boxed, and if the deadline is reached, the outer `context` object
is canceled signalling a more aggressive tear down.

```go 
   err = w.Run(context.Background(), nil)
```
 
### Hyper Scalability

`zkafka` work implementation supports a concept called `virtual partitions`. This extends
the kafka `partition` concept. Message ordering is guaranteed within a kafka partition,
and the same is true for a `virtual partition`. Every `zkafka.Work` object manages a pool
of goroutines (1 by default and controlled by `zkafka.Speedup(n int)` option) which are the `virtual partitions`. As a message is read, it is 
assigned to one of the `virtual partitions`. The decision about which is based on `hash(message.Key) % virtual partition count`. 
This is the same mechanism kafka uses. Using this strategy, a message with the same key will be assigned
to the same virtual partition. 

What this allows is for another layer of scalability. To increase throughput, and maintain the same 
message ordering guarantees, there's no longer a requirement to increase the kafka partition count (which can be operationally difficult).
Instead, use `zkafka.Speedup()` to add to the virtual partition count. 

```shell
// sets up kafka broker locally
make setup;
// terminal 1. Starts producing messages. To juice up production rate, remove the time.Sleep() in the producer and turn acks off.
make example-producer
// terminal 2. Starts a worker with speedup=5. 
make example-worker
```

### Configurable Dead Letter Topics

A `zkafka.Work` instance can be configured to write to a Dead Letter Topic (DLT) when message processing fails.
This can be accomplished `zkafka.WithDeadLetterTopic()` option. Or more conveniently, can be controlled by adding
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

The above will write to `zkafka-example-deadletter-topic` in the case of a processing error. 

The returned processor error determines whether or not a message is written to a dead letter topic. In some situations,
you might not want to route an error to a DLT. An example might be, malformed data.

The processor author has control over this behavior by way of the `zkafka.ProcessError`.

```go 
	return zkafka.ProcessError{
		Err:                 err,
		DisableDLTWrite:     true,
	}
```

The above returned error will skip writing to the DLT. 

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

### Process Delay Workers

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

    workerConfig1 := zkafka.ConsumerTopicConfig{
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

1. Be read by worker1 from `topicA`
2. If message processing fails, written to `topicB` via the DLT configuration
3. Read by worker2 from `topicB`
4. If message processing fails, written to `topicC` via the DLT configuration

This creates a retry pipeline. The issue is that worker2, ideally would process on a delay (giving whatever transient error is occurring a chance to resolve).
Luckily, `zkafka` supports such a pattern. By specifying `ProcessDelayMillis` in the config, a worker is created which will delay procesing of
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

#### Commit Strategy:

1. *Store* offset of a message for commit after processing
2. *Commit* messages whose offsets have been stored at configurable intervals (`auto.commit.interval.ms`)
3. *Commit* messages whose offsets have been stored when partitions are revoked
   (this is implicitly handled by librdkafka. To see this add debug=cgrp in ConsumerTopicConfig, and there'll be COMMIT logs after a rebalance.
   If doing this experience, set the `auto.commit.interval.ms` to a large value to avoid confusion between the rebalance commit)
4. *Commit* messages whose offsets have been stored on close of reader
   (this is implicitly handled by librdkafka. To see this add debug=cgrp in ConsumerTopicConfig, and there'll be COMMIT logs after the client is closed, but before the client is destroyed)

Errors returned on processing are still stored. This avoids issues due to poison pill messages (messages which will
never be able to be processed without error)
as well as transient errors blocking future message processing. Use WithOnDone option to register callback for
additional processing of these messages.

This strategy is based off
of [Kafka Docs - Offset Management](https://docs.confluent.io/platform/current/clients/consumer.html#offset-management)
where a strategy of asynchronous/synchronous commits is suggested to reduced duplicate messages.

### SchemaRegistry Support:

There is limited support for schema registry in zkafka. A schemaID can be hardcoded via configuration. No
communication is done with schema registry, but some primitive checks can be conducted if a schemaID is specified via
configuration.

#### Producers

Producers will include the schemaID in messages written to kafka (without any further verification).

#### Consumers

Consumers will verify that the message they're consuming has the schemaID specified in configuration
(if it's specified). Be careful here, as backwards compatible schema evolutions would be treated as an error condition
as the new schemaID wouldn't match what's in the configuration.

### Consumer/Producer Configuration

See for description of configuration options and their defaults:

1. [Consumer Configuration](https://docs.confluent.io/platform/current/installation/configuration/consumer-configs.html)
2. [Producer Configurations](https://docs.confluent.io/platform/current/installation/configuration/producer-configs.html)

These are primarily specified through the TopicConfig struct. TopicConfig includes strongly typed fields which translate
to librdconfig values. To see translation see config.go. An escape hatch is provided for ad hoc config properties via
the AdditionalProperties map. Here config values that don't have a strongly typed version in TopicConfig may be
specified. Not all specified config values will work (for example `enable.auto.commit=false` would not work with this
client because that value is explicitly set to true after reading of the AdditionalProperties map).

```json5

{
  "KafkaTopicConfig": {
    "Topic": "KafkaTopicName",
    "BootstrapServers": [
      "localhost:9092"
    ],
    // translates to librdkafka value "bootstrap.servers"
    // specify ad hoc configuration values which don't have a strongly typed version in the TopicConfig struct.
    "AdditionalProperties": {
      "auto.commit.interval.ms": 1000,
      "retry.backoff.ms": 10
    }
  }
}

```

3. zkafka.ProcessError

The `zkafka.ProcessError` can be used to control error handling on a per-message basis. Use of this type is entirely optional. The current options exposed through this type are as follows:
1. `DisableDLTWrite`: if true, the message will not be written to a dead letter topic (if one is configured)
2. `DisableCircuitBreaker`: if true, the message will not count as a failed message for purposes of controlling the circuit breaker.
