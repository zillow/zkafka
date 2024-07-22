# zkafka

[![License](https://img.shields.io/github/license/zillow/zkafka)](https://github.com/zillow/zkafka/blob/main/LICENSE)
[![GitHub Actions](https://github.com/zillow/zkafka/actions/workflows/go.yml/badge.svg)](https://github.com/zillow/zkafka/actions/workflows/go.yml)
[![Codecov](https://codecov.io/gh/zillow/zkafka/branch/main/graph/badge.svg?token=STRT8T67YP)](https://codecov.io/gh/zillow/zkafka)


## Install

`go get -u github.com/zillow/zkafka`

## About

A library built on top of confluent-kafka-go for reading and writing to kafka with limited Schema Registry support. The
library supports at least once message processing. It does so using a commit strategy built off auto commit and manual
offset storage.

---
**NOTE**

confluent-kafka-go is a CGO module, and therefore so is zkafka. When building zkafka, make sure to set
CGO_ENABLED=1.
---

There are two quick definitions important to the understanding of the commit strategy

1. **Commit** - involves communicating with kafka broker and durably persisting offsets on a kafka broker.
2. **Store** - is the action of updating a local store of message offsets which will be persisted during the commit
   action

## Commit Strategy:

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

## Work

zkafka also supports an abstraction built on top of the reader defined in the Work struct (`work.go`). Work introduces
concurrency by way of the configurable option `Speedup(n int)`. This creates n goroutines which process messages as
they are written to the golang channel assigned to that goroutine. Kafka key ordering is preserved (by a mechanism similar to kafka
partitions) whereby a message sharing the same key will always be written to the same channel (internally, this is called a virtual partition).
By default, the number of virtual partitions is equal 1. 
Speedup() can be increased beyond the number of assigned physical partitions without concern of data loss on account of the reader tracking in-work message offsets and only
committing the lowest offset to be completed. Additionally, kafka key ordering is preserved even as the number of virtual partitions increases beyond the number of physical assigned
partitions.

## SchemaRegistry Support:

There is limited support for schema registry in zkafka. A schemaID can be hardcoded via configuration. No
communication is done with schema registry, but some primitive checks can be conducted if a schemaID is specified via
configuration.

### Producers

Producers will include the schemaID in messages written to kafka (without any further verification).

### Consumers

Consumers will verify that the message they're consuming has the schemaID specified in configuration
(if it's specified). Be careful here, as backwards compatible schema evolutions would be treated as an error condition
as the new schemaID wouldn't match what's in the configuration.

## Consumer/Producer Configuration

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
      "localhost:9093"
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

## Installation

go get -u gitlab.zgtools.net/devex/archetypes/gomods/zkafka

## Running Example

```
make setup-test

// <Terminal 1>
make example-producer

// <Terminal 2>
make example-worker
```
