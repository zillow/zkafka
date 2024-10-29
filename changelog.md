# Changelog

All notable changes to this project will be documented in this file.

This project adheres to Semantic Versioning.

## 2.0.1 (October 29, 2024)

1. Updated `confluent-kafka-go` version `2.6.0` to address security vulnerability.

## 2.0.0 (October 17, 2024)

1. Removed dependency on github.com/golang/mock (deprecated) in favor of github.com/uber-go/mock 
2. Bugfixes in `WithDisableCircuitBreaker` and `WithDisableBusyLoopBreaker` options implementation

## 1.3.0 (Sep 25, 2024)

1. Added `WithDisableCircuitBreaker` and `WithDisableBusyLoopBreaker` options. These are variants of the now deprecated `DisableCircuitBreaker`
and `DisableBusyLoopBreaker` options. They provide a booling parameter which is more convenient for usage with
code generation and for shimming with configuration.

## 1.2.0 (Sep 23, 2024)

1. Update to allow subject name specification (not just TopicNameStrategy)
1. Updated `avro_schema_registry` formatter deserialization to require passed in schema (less susceptible to errors from inferred target schema) 

## 1.1.0 (Sep 23, 2024)

1. Added support for schema registry (avro, proto, json). Extended `zfmt.FormatterType` types to include `avro_schema_registry`, `proto_schema_registry` and `json_schema_registry`
2. Added lifecycle function `LifecyclePostReadImmediate`
3. Added `workFactory.CreateWithFunc` which is a convenience work factory method for creating work using a callback instead of an interface (can reduce boilerplate) in some scenarios.
4. During the creation of readers/writers an error is now returned if bootstrap servers is empty
5. Updated golang version 1.23
6. Updated otel versions


## 1.0.2 (Sep 6, 2024)

1. Updated `WithDeadLetterTopic` option to borrow username and password from ConsumerTopicConfig when those issues aren't specified on DeadLetterTopicConfig

## 1.0.1 (Sep 3, 2024)

1. Added dlt topic name in error logs on dlt write failure

## 1.0.0 (July 2024)

Initial release to public github.com

Internal Releases Below
----

## 4.2.0 (June 2024)

1. Updated otel to v1.27
1. Updated semconv to v1.25

## 4.1.0 (May 2024)
1. Added consumer delay config `ProcessDelayMillis` this allows the consumption of messages, but delays processing until at least the configured delay has passed since the message was written.  Useful for intermediate dead letter messages.
1. Added ability to add headers via writer option (WithHeaders).
1. Updated so `linger.ms=0` is the default.
1. Updated so `socket.nagle.disable=true` is the default.
1. Increased `SessionTimeoutMillis` and `MaxPollIntervalMillis` defaults to be greater than `ProcessDelayMillis` so that an inadvertent long running processor doesn't cause a rebalance.
1. Updated confluent.Config's usage of `config.AdditionalProps`. They can now override any setting. Previously, some of the promoted configs couldn't be set via this map.

## 4.0.0 (April 2024)
1. Updated to be used with zworker.Work which requires a `work.Run` interface not `work.Do`. The difference been
   `work.Run` is executed once, and `zkafka` is responsible for continuously looping, whereas `work.Do` would be continually executed
   in a loop.
1. Renamed `zkafka.WithOnDoneWithContext` to `zkafka.WithOnDone` and removed original `zkafka.WithOnDone` option (which didn't provide a context.Context arg)
1. Updated `Writer` interface to include `WriteRaw` method. The concrete type has supported it for some time, but was waiting for a major version roll to update the interface.
1. Updated `zkafka.Message` headers to be a `map[string][]byte` instead of `map[string]interface{}`. This is closer to the transport representation and is more convenient and self documenting.
The interface{} type was a holdover from the original implementation and was not necessary.
1. Removed `ExtractHeaderKeys` (reduce surface area of package). Opinionated API (zillow specific) that now resides in zkafkacomproot
1. Added variadic arguments (`...zkafka.WriteOption`) to `kwriter.Write*` methods . This allows future customization in a noninvasive way. 
1. Removed `zcommon` dependency. Introduce hooks which can be used toward the same end
1. Changed interface{} -> any
1. Added lifecycle methods `PostRead`, `PreWrite` and `PostFanout`
1. Added `WithConsumerProvider` and `WithProducerProvider` which is useful for full e2e testing. 
1. Updated work to remove read messages that won't be worked, from the inwork count. Allows Faster shutdown

## 3.0.0 (October 2023)

1. Supports migration from Datadog [statsd](https://www.datadoghq.com/blog/statsd/) to [Panoptes](https://getpanoptes.io/).
2. Removes the `Metrics` interface and related options. Removes `NoopMetrics` struct. Rather than calling metrics classes directly, the user registers lifecycle hooks and calls the metric provider from the hooks. For example, [zkafkacomproot](https://gitlab.zgtools.net/devex/archetypes/gomods/zkafkacomproot) registers hooks that call the zmetric provider.
3. Removes the `RequestContextExtractor` interface. Instead, use the `PreProcessing` lifecycle hook to extract information from the request and add it to the context. The context returned from the `PreProcessing` hook is used for the rest of the request lifecycle.

## 2.0.0 (July 27th 2023)

1. Removes the dependency on [opentracing-go](https://github.com/opentracing/opentracing-go).
   Opentracing-go was a stale dependency that was no longer receiving updates. The library is now instrumented with [opentelemtry](https://github.com/open-telemetry/opentelemetry-go)
   a stable tracing library, that was written with backwards compatability in mind with opentracing-go.
2. Removed `WithTracer(opentracing.Tracer)`. Use `WithTracerProvider` and `WithTextMapPropagator` instead.

## 1.0.0 (August 2022)

Updated to account for update in zfmt which changes the values of some of the formatter factory
entry values.

To see further details on zmt update to V1. See migration guide [here](https://gitlab.zgtools.net/devex/archetypes/gomods/zfmt/-/blob/main/README.md#migration-guide)
