package zkafka

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/sony/gobreaker"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// Work continuously reads and processes messages from the queue (or queues) it's registered to listen to.
// Work executes the following steps
// 1) Read upto MaxNumberOfMessages from the queue(s) using the provided reader.
// 2) Offload the read message to a pool (one per queue) (processing from one queue doesn't block another)
// 3) Each pool has N go routines available to it for concurrent processing. Processing involves executing external code registered via a callback
// 4) On successful processing queue item for batch deletion.
// 5) On errors, apply circuit breaker if configured.

// Work has a single public method `Run()` which continuously reads and process messages from the  topic (or topics) it is registered to listen to.
// `Run()` executes the following steps
//
//  1. Read a kafka.Message using the provided reader.
//  2. Select the virtual partition pool allocated for a specific topic
//  3. Select and write the `kafka.Message` to the pool's virtual partition based on a hash of the `kafka.Message.Key` (virtual partition selection)
//  4. A goroutine is assigned for each virtual partition. Its responsibility is to continuously read from its virtual partition, call the Process callback function, and then store the offset of the message.
//
// Additional responsibilities includes:
//  1. Logging
//  2. Executing callbacks of special lifecycle events (useful for observability like tracing/metrics)
//  3. Tripping circuit breaker for error conditions
//  4. Writing to dead letter topic, when configured.
type Work struct {
	// kafka topic configuration
	topicConfig ConsumerTopicConfig

	kafkaProvider ClientProvider

	// each message will be passed to this processor in a separate go routine.
	processor processor

	// logger
	logger Logger

	// metrics are handled in the lifecycle hooks.
	lifecycle LifecycleHooks

	// reader is created once per work.
	rdrMtx sync.RWMutex
	reader Reader

	// poolSize is how many goroutines can process messages simultaneously.
	// It defines the worker pool size.
	// default is 1.
	// Use Speedup to control this option
	poolSize *uint16

	messageBuffer chan struct{}

	// virtualPartitions are a list of channels each with an assigned worker goroutine.
	// Each message is passed to a virtual partition based on the hash of the message key.
	// The same message key will always be written to the same virtual partition. The virtual partition
	// extends the concept of a kafka partition to a set of in memory channels.
	virtualPartitions []chan workUnit

	// Circuit breaker to throttle reading from the topic.
	cb *gobreaker.TwoStepCircuitBreaker

	// Number of consecutive failures allowed before opening the circuit.
	// Use CircuitBreakAfter to control
	cbAfter *uint32

	// Duration for which a circuit is open. Use CircuitBreakFor to control
	cbFor *time.Duration

	// Disable circuit breaking. Use DisableCircuitBreaker to control
	disableCb bool

	// Busy loop breaker. When circuit breaker circuit is open, instead of consuming cpu in a busy loop
	// We just block "Do" for the amount of time circuit is going to be open.
	// This prevents immediate subsequent calls to Do which we know are going to be noop since circuit is open.
	blb busyLoopBreaker

	// onDones is a list of optional callbacks that are called after the processing of a message
	onDones []func(ctx context.Context, message *Message, err error)

	tracer trace.Tracer
	p      propagation.TextMapPropagator
}

type processorError struct {
	inner error
}

func (w processorError) Error() string {
	return fmt.Sprintf("error returned from processor: %s", w.inner.Error())
}

func (w processorError) Unwrap() error {
	return w.inner
}

// Run executes a pipeline with a single reader (possibly subscribed to multiple topics)
// fanning out read messages to virtual partitions (preserving message order) and subsequently being processed
// by the registered processor (user code which executes per kafka message).
//
// It returns either after context.Context cancellation or receiving a shutdown signal from settings (both of which
// will cause the awaited execReader and execProcessor methods to return
func (w *Work) Run(ctx context.Context, shutdown <-chan struct{}) error {
	w.initiateProcessors(ctx)

	g := errgroup.Group{}
	g.Go(func() error {
		w.execReader(ctx, shutdown)
		w.logger.Debugw(ctx, "exiting reader loop")
		return nil
	})
	g.Go(func() error {
		w.execProcessors(ctx, shutdown)
		w.logger.Debugw(ctx, "exiting processors loop")
		return nil
	})
	return g.Wait()
}

func (w *Work) execReader(ctx context.Context, shutdown <-chan struct{}) {
	defer func() {
		w.closeProcessors(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-shutdown:
			return
		default:
			w.fanOut(ctx, shutdown)
		}
		if w.lifecycle.PostFanout != nil {
			w.lifecycle.PostFanout(ctx)
		}
	}
}

// execProcessors checks whether the worker pool is in started state or not. If it isn't
// it starts the worker pool in a separate goroutine. The "worker pool" is a collection of goroutines each individually processing a
// virtual partition. When the worker pool goroutine exists, the flag is flipped again, allowing
// for a restart
func (w *Work) execProcessors(ctx context.Context, shutdown <-chan struct{}) {
	wg := sync.WaitGroup{}
	wg.Add(len(w.virtualPartitions))
	for i := range w.virtualPartitions {
		i := i
		go func() {
			w.processVirtualPartition(ctx, i, shutdown)
			wg.Done()
		}()
	}
	wg.Wait()
}

// initiateProcessors creates a buffered channel for each virtual partition, of size poolSize. That way
// a particular virtual partition never blocks because of its own capacity issue (and instead the goroutinepool is used
// to limit indefinte growth of processing goroutines).
func (w *Work) initiateProcessors(_ context.Context) {
	poolSize := w.getPoolSize()
	w.virtualPartitions = make([]chan workUnit, poolSize)
	for i := 0; i < int(poolSize); i++ {
		w.virtualPartitions[i] = make(chan workUnit, poolSize)
	}
}

func (w *Work) closeProcessors(_ context.Context) {
	for _, p := range w.virtualPartitions {
		close(p)
	}
}

func (w *Work) fanOut(ctx context.Context, shutdown <-chan struct{}) {
	successFunc, err := w.cb.Allow()
	// If circuit is open, Allow() returns error.
	// If circuit is open, we don't read.
	if err != nil {
		w.logger.Warnw(ctx, "Kafka topic processing circuit open",
			"topics", w.topicConfig.topics())

		blocker, cleanup := w.blb.wait()
		select {
		case <-blocker:
		case <-ctx.Done():
			cleanup()
		}
		return
	}
	msg, err := w.readMessage(ctx, shutdown)

	if w.lifecycle.PostReadImmediate != nil {
		w.lifecycle.PostReadImmediate(ctx, LifecyclePostReadImmediateMeta{
			Message: msg,
			Err:     err,
		})
	}

	if err != nil {
		w.logger.Warnw(ctx, "Kafka worker read message failed",
			"error", err,
			"topics", w.topicConfig.topics())
		successFunc(false)
		return
	}

	if msg == nil {
		successFunc(true)
		return
	}
	if w.lifecycle.PostRead != nil {
		ctx, err = w.lifecycle.PostRead(ctx, LifecyclePostReadMeta{
			Topic:   msg.Topic,
			GroupID: msg.GroupID,
			Message: msg,
		})
		if err != nil {
			w.logger.Warnw(ctx, "Error in post read callback in worker", "offset", msg.Offset, "partition", msg.Partition, "topic", msg.Topic, "groupID", msg.GroupID)
		}
	}
	w.logger.Debugw(ctx, "Kafka topic message received", "offset", msg.Offset, "partition", msg.Partition, "topic", msg.Topic, "groupID", msg.GroupID)

	index, err := selectPartitionIndex(msg.Key, msg.isKeyNil, len(w.virtualPartitions))
	if err != nil {
		// selectPartitionIndex should never return errors (as long as len(w.virtualPartitions) > 0 which should always be the case
		w.logger.Warnw(ctx, "Failed to selected virtual partition index. Choosing 0 index since it is guaranteed to exist", "error", err)
		index = 0
	}
	select {
	case w.messageBuffer <- struct{}{}:
		select {
		case w.virtualPartitions[index] <- workUnit{
			ctx:         ctx,
			msg:         msg,
			successFunc: successFunc,
		}:
		case <-ctx.Done():
			w.removeInWork(msg)
		}
	case <-shutdown:
		w.removeInWork(msg)
		break
	case <-ctx.Done():
		w.removeInWork(msg)
		break
	}
}

func (w *Work) readMessage(ctx context.Context, shutdown <-chan struct{}) (*Message, error) {
	if err := w.ensureReader(ctx); err != nil {
		return nil, err
	}
	msg, err := w.reader.Read(ctx)
	if err != nil {
		return nil, err
	}
	select {
	case <-shutdown:
		w.removeInWork(msg)
		return nil, nil
	case <-ctx.Done():
		w.removeInWork(msg)
		return nil, nil
	default:
		break
	}
	return msg, nil
}

// removeInWork is a cleanup function used when messages have been read
// but because wrapup is occurring are decidedly not processed.
// Internally, in progress work is tracked by commit managers and exit is delayed if they're not empty.
// It should be called when ctx cancellation or shutdown signal closure is causing work.Run exit, and a message has been read
func (w *Work) removeInWork(msg *Message) {
	if msg == nil {
		return
	}
	reader, ok := w.reader.(*KReader)
	if ok && reader != nil {
		reader.removeInWork(msg.topicPartition)
	}
}

// processVirtualPartition indefinitely listens for new work units written to its managed partition.
// As they become available, it processes them. Additionally, it is responsible for exiting for context cancellation.
//
// Its important that every message read from the partition is also released from the messageBuffer. Because processSingle has panic recovery,
// and shouldNotCircuitBreak is a tested library function, we can be sure that every message read from the partition will be released from the messageBuffer.
// If this invariant is broken, we could reduce throughput because it is limited by the availability in the messageBuffer
func (w *Work) processVirtualPartition(ctx context.Context, partitionIndex int, shutdown <-chan struct{}) {
	partition := w.virtualPartitions[partitionIndex]
	delayCalc := delayCalculator{}
	for {
		select {
		case <-ctx.Done():
			return
		case unit, ok := <-partition:
			// partition has been closed and we should exit
			if !ok {
				return
			}
			msg := unit.msg
			if msg == nil {
				continue
			}

			remainingDelay := delayCalc.remaining(w.processDelay(), msg.TimeStamp)
			if !w.execDelay(ctx, shutdown, remainingDelay) {
				// while waiting for processDelay we received some shutdown signal, so the message should be removed from in flight,
				// so it doesn't block during final rebalance
				w.removeInWork(msg)
				continue
			}
			err := w.processSingle(unit.ctx, msg, partitionIndex)
			unit.successFunc(shouldNotCircuitBreak(err))
			<-w.messageBuffer
		}
	}
}

func (w *Work) processDelay() time.Duration {
	if w.topicConfig.ProcessDelayMillis == nil || *w.topicConfig.ProcessDelayMillis <= 0 {
		return 0
	}
	return time.Duration(*w.topicConfig.ProcessDelayMillis) * time.Millisecond
}

// execDelay blocks, when given a positive processDelay, until that processDelay duration has passed or a signal indicates message processing should begin to exit exited
func (w *Work) execDelay(ctx context.Context, shutdown <-chan struct{}, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}
	select {
	case <-ctx.Done():
		return false
	case <-shutdown:
		return false
	case <-time.After(delay):
		return true
	}
}

func (w *Work) processSingle(ctx context.Context, msg *Message, partitionIndex int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			// in case of panic, we want to confirm message is marked as done. Most of the time this will be redundant,
			// but it's possible that Done hasn't been called if panic happens during custom GetRequestContext extraction.
			// It's safe to call Done multiple times
			msg.Done()
			// Panic for one message should not bring down the worker. Log and continue
			w.logger.Errorw(ctx, "Kafka topic single message processing panicked",
				"recover", r,
				"kmsg", msg,
			)
			switch x := r.(type) {
			case error:
				err = x
			default:
				err = errors.New("kafka topic single message processing panicked")
			}
		}
	}()

	// send the done signal. Always do this. Otherwise, the message won't be committed
	defer func() {
		msg.DoneWithContext(ctx)
		for _, onDone := range w.onDones {
			if onDone != nil {
				onDone(ctx, msg, err)
			}
		}
	}()

	ctx = w.lifecyclePreProcessing(ctx, msg, partitionIndex)
	ctx, span := w.startSpan(ctx, msg)
	defer span.End()

	ctxCancel, cancel := context.WithTimeout(ctx, w.processTimeoutDuration())
	defer cancel()

	// In the case of a timeout we'll return an error indicating a timeout occurred.
	// Additionally, the context will be canceled, so the processor is told it should release resources.
	// If no timeout occurred, we'll return the processor result (either an error or nil).
	err = func() error {
		processResponses := make(chan error, 1)

		go func() {
			defer func() {
				if r := recover(); r != nil {
					// Panic for one message should not bring down the worker. Log and continue
					w.logger.Errorw(ctx, "Kafka topic single message processing panicked",
						"recover", r,
						"kmsg", msg,
					)
					switch x := r.(type) {
					case error:
						processResponses <- x
					default:
						processResponses <- errors.New("kafka topic single message processing panicked")
					}
				}
			}()

			begin := time.Now()
			e := w.processor.Process(ctxCancel, msg)
			w.lifecyclePostProcessing(ctx, msg, partitionIndex, begin, e)

			if e != nil {
				processResponses <- processorError{inner: e}
			} else {
				processResponses <- nil
			}
		}()

		select {
		case err2 := <-processResponses:
			return err2
		case <-ctxCancel.Done():
			if errors.Is(ctxCancel.Err(), context.DeadlineExceeded) {
				return errors.New("timeout occurred during kafka process")
			}
			if errors.Is(ctxCancel.Err(), context.Canceled) {
				// an outside context will typically be canceled because of a sigterm or siginterrupt. This is often
				// part of a natural teardown, and we won't error on this condition
				x := ctxCancel.Err()
				w.logger.Warnw(ctx, "Outside context canceled", "kmsg", msg, "error", x)
				return nil
			}
			return fmt.Errorf("processSingle execution canceled: %w", ctxCancel.Err())
		}
	}()

	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())

	if pError, ok := err.(processorError); ok {
		// Because we assume workers will log their own internal errors once
		// already, we try to ignore logging them twice by also logging them
		// as errors in zkafka (also as this is not considered an 'error'
		// in the zkafka library itself).
		w.logger.Warnw(ctxCancel, "Kafka topic single message processing failed",
			"error", pError.inner,
			"kmsg", msg,
		)
	} else {
		w.logger.Errorw(ctxCancel, "Kafka topic single message processing failed",
			"error", err,
			"kmsg", msg,
		)
	}
	return err
}

func (w *Work) lifecyclePreProcessing(ctx context.Context, msg *Message, partitionIndex int) context.Context {
	if w.lifecycle.PreProcessing != nil {
		lcPreMeta := LifecyclePreProcessingMeta{
			Topic:                 msg.Topic,
			GroupID:               w.topicConfig.GroupID,
			VirtualPartitionIndex: partitionIndex,
			TopicLag:              time.Since(msg.TimeStamp),
			Message:               msg,
		}

		var err error
		ctx, err = w.lifecycle.PreProcessing(ctx, lcPreMeta)
		if err != nil {
			w.logger.Warnw(ctx, "Lifecycle pre-processing failed", "error", err, "meta", lcPreMeta)
		}
	}

	return ctx
}

func (w *Work) lifecyclePostProcessing(ctx context.Context, msg *Message, partitionIndex int, begin time.Time, respErr error) {
	if w.lifecycle.PostProcessing != nil {
		lcPostMeta := LifecyclePostProcessingMeta{
			Topic:                 msg.Topic,
			GroupID:               w.topicConfig.GroupID,
			VirtualPartitionIndex: partitionIndex,
			ProcessingTime:        time.Since(begin),
			Msg:                   msg,
			ResponseErr:           respErr,
		}

		lcErr := w.lifecycle.PostProcessing(ctx, lcPostMeta)
		if lcErr != nil {
			w.logger.Warnw(ctx, "Lifecycle post-processing failed", "error", lcErr, "meta", lcPostMeta)
		}
	}
}

func (w *Work) startSpan(ctx context.Context, msg *Message) (context.Context, spanWrapper) {
	if msg == nil || w.tracer == nil {
		return ctx, spanWrapper{}
	}
	carrier := &msgCarrier{msg: msg}

	if w.p != nil {
		ctx = w.p.Extract(ctx, carrier)
	}

	// https://opentelemetry.io/docs/specs/semconv/attributes-registry/messaging/
	const base10 = 10
	offset := strconv.FormatInt(msg.Offset, base10)
	opts := []trace.SpanStartOption{
		trace.WithAttributes(
			semconv.MessagingSystemKafka,
			semconv.MessagingMessageID(offset),
			semconv.MessagingKafkaConsumerGroup(w.topicConfig.GroupID),
			semconv.MessagingDestinationName(msg.Topic),
			semconv.MessagingKafkaDestinationPartition(int(msg.Partition)),
			semconv.MessagingKafkaMessageKey(msg.Key),
			semconv.MessagingKafkaMessageOffset(int(msg.Offset)),
		),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}

	operationName := "zkafka.process"
	ctx, otelSpan := w.tracer.Start(ctx, operationName, opts...)

	return ctx, spanWrapper{otelSpan}
}

func (w *Work) processTimeoutDuration() time.Duration {
	if w.topicConfig.ProcessTimeoutMillis == nil {
		return 60 * time.Second
	}
	return time.Duration(*w.topicConfig.ProcessTimeoutMillis) * time.Millisecond
}

func (w *Work) getPoolSize() uint16 {
	if w.poolSize == nil || *w.poolSize <= 0 {
		return 1
	}
	return *w.poolSize
}

// ensureReader creates a KafkaReader and sets it to the Work field value.
// we don't do this in a sync.Once because during spinup sometimes things aren't ready and errors are returned, and
// we want to do a true retry instead of returning a cached error
func (w *Work) ensureReader(ctx context.Context) error {
	w.rdrMtx.RLock()
	if w.reader != nil {
		w.rdrMtx.RUnlock()
		return nil
	}
	w.rdrMtx.RUnlock()

	w.rdrMtx.Lock()
	defer w.rdrMtx.Unlock()

	rdr, err := w.kafkaProvider.Reader(ctx, w.topicConfig)
	if err != nil {
		return err
	}

	if rdr == nil {
		return errors.New("nil reader received")
	}

	w.reader = rdr

	return nil
}

// ProcessError wraps an error that a processor encounters, while also exposing
// controls that allow for specifying how the error should be handled.
type ProcessError struct {
	// Err is the actual error that the processor encountered.
	Err error
	// DisableCircuitBreak indicates that this error should be ignored for
	// purposes of managing the circuit breaker. Any returned errors where
	// this is set to true will not cause the processing of messages to slow.
	DisableCircuitBreak bool
	// DisableDLTWrite indicates that this message should not be written to
	// a dead letter topic (if one is configured) as it cannot be retried
	// successfully.
	DisableDLTWrite bool
}

func (p ProcessError) Error() string {
	return fmt.Sprintf("err: %s", p.Err.Error())
}

func (p ProcessError) Unwrap() error {
	return p.Err
}

// shouldCircuitBreak checks for our bespoke error type, and if it is any other
// type ultimately results in just a nil check.
func shouldNotCircuitBreak(err error) bool {
	// we check this in any case to avoid typed nil gotchas
	if err == nil {
		return true
	}

	processError := &ProcessError{}
	if ok := errors.As(err, processError); ok {
		return processError.DisableCircuitBreak
	}

	return false
}

type processor interface {
	// Process is called for each kafka message read.
	Process(ctx context.Context, message *Message) error
}

// WorkFactory creates a work object which reads messages from kafka topic and processes messages concurrently.
type WorkFactory struct {
	kafkaProvider ClientProvider
	logger        Logger
	tp            trace.TracerProvider
	p             propagation.TextMapPropagator
	lifecycle     LifecycleHooks
}

// NewWorkFactory initializes a new WorkFactory
func NewWorkFactory(
	kafkaProvider ClientProvider,
	options ...WorkFactoryOption,
) WorkFactory {
	factory := WorkFactory{
		kafkaProvider: kafkaProvider,
		logger:        NoopLogger{},
	}

	for _, option := range options {
		if option != nil {
			option.apply(&factory)
		}
	}
	return factory
}

func (f WorkFactory) CreateWithFunc(topicConfig ConsumerTopicConfig, p func(_ context.Context, msg *Message) error, options ...WorkOption) *Work {
	return f.Create(topicConfig, processorAdapter{p: p}, options...)
}

// Create creates a new Work instance.
func (f WorkFactory) Create(topicConfig ConsumerTopicConfig, processor processor, options ...WorkOption) *Work {
	work := &Work{
		topicConfig:   topicConfig,
		kafkaProvider: f.kafkaProvider,
		processor:     processor,
		logger:        f.logger,
		lifecycle:     f.lifecycle,
		tracer:        getTracer(f.tp),
		p:             f.p,
	}

	if topicConfig.DeadLetterTopicConfig != nil {
		cfg := *topicConfig.DeadLetterTopicConfig
		if cfg.ClientID == "" {
			cfg.ClientID = topicConfig.ClientID
		}
		options = append(options, WithDeadLetterTopic(cfg))
	}

	for _, option := range options {
		option.apply(work)
	}

	poolSize := work.getPoolSize()
	work.messageBuffer = make(chan struct{}, poolSize)

	cbSetting := gobreaker.Settings{}

	if work.disableCb {
		cbSetting.ReadyToTrip = func(gobreaker.Counts) bool { return false }
	} else {
		if work.cbFor != nil && *work.cbFor > 0 {
			cbSetting.Timeout = *work.cbFor
		}
		if work.cbAfter != nil && *work.cbAfter > 0 {
			cbSetting.ReadyToTrip = func(c gobreaker.Counts) bool { return c.ConsecutiveFailures >= *work.cbAfter }
		}
		b := cbSetting.Timeout
		if b == 0 {
			b = 60 * time.Second
		}
		work.blb.maxPause = b
		cbSetting.OnStateChange = func(name string, from, to gobreaker.State) {
			switch to {
			case gobreaker.StateOpen:

				// returned timer ignored. have no need to call Stop on it anyplace yet.
				_ = time.AfterFunc(b, func() { work.blb.release() })
			case gobreaker.StateClosed:
				work.blb.release()
			}
		}
	}
	work.cb = gobreaker.NewTwoStepCircuitBreaker(cbSetting)

	return work
}

type busyLoopBreaker struct {
	disabled bool
	mtx      sync.Mutex
	waiters  []chan struct{}
	// maxPause indicates the max amount of a time a busyLoopBreaker will wait at the wait()
	// site before returning. Can be used to guarantee that wait() doesn't block indefinitely
	maxPause time.Duration
}

func (b *busyLoopBreaker) wait() (<-chan struct{}, func()) {
	if b.disabled {
		closedCh := make(chan struct{})
		close(closedCh)
		return closedCh, func() {}
	}
	c := make(chan struct{})
	b.mtx.Lock()
	b.waiters = append(b.waiters, c)
	b.mtx.Unlock()

	timer := time.AfterFunc(b.maxPause, b.release)

	return c, func() {
		// if wait is released externally, we'll want to release this timer's resources
		timer.Stop()
	}
}

func (b *busyLoopBreaker) release() {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	for _, v := range b.waiters {
		close(v)
	}
	b.waiters = nil
}

func selectPartitionIndex(key string, isKeyNil bool, partitionCount int) (int, error) {
	if partitionCount <= 0 {
		return 0, errors.New("partitionCount must be greater than 0")
	}
	if isKeyNil {
		key = uuid.NewString()
	}
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		return 0, fmt.Errorf("failed to create partition index from seed string: %w", err)
	}
	index := int(h.Sum32())
	return index % partitionCount, nil
}

// workUnit encapsulates the work being written to a virtual partition. It includes
// the context passed in that current iteration of fanOut(), the kafka message to be processed and the
// successFunc callback to be called when the work is done (indicating success or failure)
type workUnit struct {
	ctx         context.Context
	msg         *Message
	successFunc func(bool)
}

type spanWrapper struct {
	span trace.Span
}

func (s spanWrapper) End(options ...trace.SpanEndOption) {
	if s.span == nil {
		return
	}
	s.span.End(options...)
}

func (s spanWrapper) RecordError(err error, options ...trace.EventOption) {
	if s.span == nil {
		return
	}
	s.span.RecordError(err, options...)
}

func (s spanWrapper) SetStatus(code codes.Code, description string) {
	if s.span == nil {
		return
	}
	s.span.SetStatus(code, description)
}

func (s spanWrapper) SetAttributes(kv ...attribute.KeyValue) {
	if s.span == nil {
		return
	}
	s.span.SetAttributes(kv...)
}

func ptr[T any](v T) *T {
	return &v
}

type delayCalculator struct {
	getNow func() time.Time
}

// remaining calculates the remaining delay which hasn't been observed by subtracting the observed delay (using now-msgTimestamp) from some `target` delay.
//
// example:
// now=3:53, w.processDelay=5s
// timestamp=2:00 -> 0s delay. (delayed long enough). remainingDelay=5s - (3:53 - 2:00) => -1:52:55s. A negative processDelay doesn't end up pausing
// timestamp=3:48 => 0s delay. remainingDelay=5s-(3:53-3:48) =>0s. A 0 (more accurately <=0) processDelay doesn't end up pausing
// timestamp=3:49 => 1s delay. remainingDelay=5s-(3:53-3:49) => 1s
// timestamp=3:53 => 5s delay.
// timestamp:3:54 => 5s delay.
// timestamp:4:00 => 5s delay (the result is capped by the `targetDelay`
func (c *delayCalculator) remaining(targetDelay time.Duration, msgTimeStamp time.Time) time.Duration {
	if c.getNow == nil {
		c.getNow = time.Now
	}
	now := c.getNow()
	observedDelay := now.Sub(msgTimeStamp)
	// this piece makes sure the return isn't possibly greater than the target
	return min(targetDelay-observedDelay, targetDelay)
}

var _ processor = (*processorAdapter)(nil)

type processorAdapter struct {
	p func(_ context.Context, msg *Message) error
}

func (a processorAdapter) Process(ctx context.Context, message *Message) error {
	return a.p(ctx, message)
}
