package zkafka

import (
	"context"
	"errors"
	"time"
)

// WorkOption interface to identify functional options
type WorkOption interface {
	apply(s *Work)
}

// Speedup increases the concurrencyFactor for a worker.
// concurrencyFactor is how many go routines can be running in parallel.
// NOTE: it's strongly recommended to add more worker instances rather than using this option to speed up each worker.
func Speedup(times uint16) WorkOption { return speedupOption{times: times} }

// CircuitBreakAfter these many consecutive failures
func CircuitBreakAfter(times uint32) WorkOption {
	return circuitBreakAfterOption{times: times}
}

// CircuitBreakFor sets the duration for which to keep the circuit open once broken
func CircuitBreakFor(duration time.Duration) WorkOption {
	return circuitBreakForOption{duration: duration}
}

// Deprecated: DisableCircuitBreaker disables the circuit breaker so that it never breaks
func DisableCircuitBreaker() WorkOption {
	return WithDisableCircuitBreaker(true)
}

// WithDisableCircuitBreaker allows the user to control whether circuit breaker is disabled or not
func WithDisableCircuitBreaker(isDisabled bool) WorkOption {
	return disableCbOption{disabled: isDisabled}
}

// Deprecated: DisableBusyLoopBreaker disables the busy loop breaker which would block subsequent read calls till the circuit re-closes.
// Without blb we see increased cpu usage when circuit is open
func DisableBusyLoopBreaker() WorkOption {
	return WithDisableBusyLoopBreaker(true)
}

// WithDisableBusyLoopBreaker disables the busy loop breaker which would block subsequent read calls till the circuit re-closes.
// Without blb we see increased cpu usage when circuit is open
func WithDisableBusyLoopBreaker(isDisabled bool) WorkOption {
	return disableBlbOption{disabled: isDisabled}
}

// WithOnDone allows you to specify a callback function executed after processing of a kafka message
func WithOnDone(f func(ctx context.Context, message *Message, err error)) WorkOption {
	return onDoneOption{f: f}
}

func WithLifecycleHooks(h LifecycleHooks) WorkOption {
	return lifeCycleOption{lh: h}
}

// WithDeadLetterTopic allows you to specify a dead letter topic to forward messages to when work processing fails
func WithDeadLetterTopic(deadLetterTopicConfig ProducerTopicConfig) WorkOption {
	return dltOption{dltConfig: deadLetterTopicConfig}
}

type speedupOption struct{ times uint16 }

func (s speedupOption) apply(w *Work) {
	if s.times > 0 {
		w.poolSize = &s.times
	}
}

type circuitBreakAfterOption struct{ times uint32 }

func (c circuitBreakAfterOption) apply(w *Work) {
	if c.times > 0 {
		w.cbAfter = &c.times
	}
}

type circuitBreakForOption struct{ duration time.Duration }

func (c circuitBreakForOption) apply(w *Work) {
	if c.duration > 0 {
		w.cbFor = &c.duration
	}
}

type disableCbOption struct {
	disabled bool
}

func (d disableCbOption) apply(w *Work) {
	w.disableCb = d.disabled
}

type onDoneOption struct {
	f func(ctx context.Context, message *Message, err error)
}

func (d onDoneOption) apply(w *Work) {
	if d.f != nil {
		w.onDones = append(w.onDones, d.f)
	}
}

type lifeCycleOption struct {
	lh LifecycleHooks
}

func (o lifeCycleOption) apply(w *Work) {
	w.lifecycle = o.lh
}

type disableBlbOption struct {
	disabled bool
}

func (d disableBlbOption) apply(w *Work) {
	w.blb.disabled = d.disabled
}

type dltOption struct {
	dltConfig ProducerTopicConfig
}

func (d dltOption) apply(w *Work) {
	f := func(ctx context.Context, message *Message, errProc error) {
		if message == nil {
			return
		}

		// if not specified explicitly in dlt config, use username/pw from consumerTopicConfig
		dltConfig := d.dltConfig
		if dltConfig.SaslUsername == nil || *d.dltConfig.SaslUsername == "" {
			dltConfig.SaslUsername = w.topicConfig.SaslUsername
		}

		if dltConfig.SaslPassword == nil || *d.dltConfig.SaslPassword == "" {
			dltConfig.SaslPassword = w.topicConfig.SaslPassword
		}
		// even if we're going to skip forwarding a message to the DLT (because there was no error),
		// establish a writer to the DLT early, so when the time comes the write is fast
		writer, err := w.kafkaProvider.Writer(ctx, dltConfig)
		if err != nil {
			w.logger.Errorw(ctx, "Failed to get writer for dlt", "error", err, "offset", message.Offset, "partition", message.Partition, "source_topic", message.Topic, "dlt_topic", d.dltConfig.Topic)
			return
		}

		// only write to dlt if an error occurred
		if errProc == nil {
			return
		}

		processError := ProcessError{}
		if ok := errors.As(errProc, &processError); ok {
			if processError.DisableDLTWrite {
				return
			}
		}

		if _, err := writer.WriteRaw(ctx, &message.Key, message.value); err != nil {
			w.logger.Errorw(ctx, "Failed to forward to DLT", "error", err, "offset", message.Offset, "partition", message.Partition, "source_topic", message.Topic, "dlt_topic", d.dltConfig.Topic)
		}
	}
	w.onDones = append(w.onDones, f)
}
