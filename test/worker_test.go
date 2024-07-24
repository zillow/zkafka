package test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/zillow/zfmt"
	"github.com/zillow/zkafka"
	zkafka_mocks "github.com/zillow/zkafka/mocks"
	"golang.org/x/sync/errgroup"

	"github.com/golang/mock/gomock"
)

const (
	topicName = "orange"
)

func TestWork_Run_FailsWithLogsWhenFailedToGetReader(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka_mocks.NewMockLogger(ctrl)
	l.EXPECT().Debugw(gomock.Any(), gomock.Any()).AnyTimes()
	l.EXPECT().Warnw(gomock.Any(), "Kafka worker read message failed", "error", gomock.Any(), "topics", gomock.Any()).MinTimes(1)
	l.EXPECT().Warnw(gomock.Any(), "Kafka topic processing circuit open", "topics", gomock.Any()).AnyTimes()

	cp := zkafka_mocks.NewMockClientProvider(ctrl)
	cp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(nil, errors.New("no kafka client reader created")).MinTimes(1)

	kwf := zkafka.NewWorkFactory(cp, zkafka.WithLogger(l))
	fanOutCount := atomic.Int64{}
	w := kwf.Create(zkafka.ConsumerTopicConfig{Topic: topicName},
		&fakeProcessor{},
		zkafka.WithLifecycleHooks(zkafka.LifecycleHooks{PostFanout: func(ctx context.Context) {
			fanOutCount.Add(1)
		}}))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return fanOutCount.Load() >= 1
	}, pollOpts{
		exit: cancel,
		timeoutExit: func() {
			require.Fail(t, "Polling condition not met prior to test timeout")
		},
		pollPause: time.Millisecond,
		maxWait:   10 * time.Second,
	})
	cancel()
	require.NoError(t, grp.Wait())
}

func TestWork_Run_FailsWithLogsWhenGotNilReader(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka_mocks.NewMockLogger(ctrl)
	l.EXPECT().Warnw(gomock.Any(), "Kafka worker read message failed", "error", gomock.Any(), "topics", gomock.Any()).Times(1)
	l.EXPECT().Debugw(gomock.Any(), gomock.Any()).AnyTimes()

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(nil, nil)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))
	w := kwf.Create(zkafka.ConsumerTopicConfig{Topic: topicName}, &fakeProcessor{},
		zkafka.WithLifecycleHooks(zkafka.LifecycleHooks{PostFanout: func(ctx context.Context) {
			cancel()
		}}))

	err := w.Run(ctx, nil)
	require.NoError(t, err)
}

func TestWork_Run_FailsWithLogsForReadError(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka_mocks.NewMockLogger(ctrl)
	l.EXPECT().Warnw(gomock.Any(), "Kafka worker read message failed", "error", gomock.Any(), "topics", gomock.Any()).MinTimes(1)
	l.EXPECT().Debugw(gomock.Any(), gomock.Any()).AnyTimes()

	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).Times(1).Return(nil, errors.New("error occurred during read"))
	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(r, nil)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))
	w := kwf.Create(zkafka.ConsumerTopicConfig{Topic: topicName}, &fakeProcessor{},
		zkafka.WithLifecycleHooks(zkafka.LifecycleHooks{PostFanout: func(ctx context.Context) {
			cancel()
		}}))

	err := w.Run(ctx, nil)
	require.NoError(t, err)
}

func TestWork_Run_CircuitBreakerOpensOnReadError(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).AnyTimes().Return(nil, errors.New("error occurred during read"))

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(r, nil)

	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))

	cnt := atomic.Int64{}
	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		&fakeProcessor{},
		zkafka.CircuitBreakAfter(1), // Circuit breaks after 1 error.
		zkafka.CircuitBreakFor(50*time.Millisecond),
		zkafka.WithLifecycleHooks(zkafka.LifecycleHooks{PostFanout: func(ctx context.Context) {
			l.Warnw(ctx, "Fan out callback called")
			cnt.Add(1)
		}}))

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return cnt.Load() >= 10
	}, pollOpts{
		exit: cancel,
		timeoutExit: func() {
			require.Failf(t, "Polling condition not met prior to test timeout", "Processing count %d", 10)
		},
	})
	require.GreaterOrEqual(t, time.Since(start), 150*time.Millisecond, "Every circuit breaker stoppage is 50ms, and we expect it to be in open state (stoppage) for half the messages  (and half open for the other half, 1 message through). (10/2-1)*50ms = 200ms. Subtract 50ms for fuzz")
	require.NoError(t, grp.Wait())
}

func TestWork_Run_CircuitBreaksOnProcessError(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key: ptr("1"),
		Fmt: &zfmt.JSONFormatter{},
	})
	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).AnyTimes().Return(msg, nil)

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).AnyTimes().Return(r, nil)

	kproc := &fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return errors.New("KafkaError.Process error")
		},
	}

	cnt := atomic.Int64{}
	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))
	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		kproc,
		zkafka.CircuitBreakAfter(1), // Circuit breaks after 1 error.
		zkafka.CircuitBreakFor(50*time.Millisecond),
		zkafka.WithOnDone(func(ctx context.Context, message *zkafka.Message, err error) {
			cnt.Add(1)
		}),
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	start := time.Now()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return cnt.Load() >= 10
	}, pollOpts{
		exit: cancel,
		timeoutExit: func() {
			require.Failf(t, "Polling condition not met prior to test timeout", "Processing count %d", 10)
		},
	})

	require.GreaterOrEqual(t, time.Since(start), 400*time.Millisecond, "Every circuit breaker stoppage is 50ms, and we expect it to be executed for each of the n -2 failed messages (first one results in error and trips the circuit breaker. Second message read prior to trip")
	require.NoError(t, grp.Wait())
}

func TestWork_Run_DoNotSkipCircuitBreak(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	failureMessage := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key: ptr("1"),
		Fmt: &zfmt.JSONFormatter{},
	})
	r := zkafka_mocks.NewMockReader(ctrl)

	r.EXPECT().Read(gomock.Any()).Return(failureMessage, nil).AnyTimes()

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).AnyTimes().Return(r, nil)

	kproc := &fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return zkafka.ProcessError{
				Err:                 errors.New("kafka.ProcessError"),
				DisableCircuitBreak: false,
			}
		},
	}

	cnt := atomic.Int64{}
	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))
	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		kproc,
		zkafka.CircuitBreakAfter(1), // Circuit breaks after 1 error.
		zkafka.CircuitBreakFor(50*time.Millisecond),
		zkafka.WithOnDone(func(ctx context.Context, _ *zkafka.Message, _ error) {
			cnt.Add(1)
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return cnt.Load() > 10
	}, pollOpts{
		exit:      cancel,
		pollPause: time.Microsecond * 100,
		timeoutExit: func() {
			require.Failf(t, "Polling condition not met prior to test timeout", "Processing count %d", 10)
		},
	})
	require.GreaterOrEqual(t, time.Since(start), 450*time.Millisecond, "Every circuit breaker stoppage is 50ms, and we expect it to be executed for each of the n -1 failed messages (first one results in error and trips the circuit breaker")
	require.NoError(t, grp.Wait())
}

func TestWork_Run_DoSkipCircuitBreak(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	failureMessage := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key: ptr("1"),
		Fmt: &zfmt.JSONFormatter{},
	})

	r := zkafka_mocks.NewMockReader(ctrl)

	r.EXPECT().Read(gomock.Any()).Return(failureMessage, nil).AnyTimes()

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).AnyTimes().Return(r, nil)

	kproc := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return zkafka.ProcessError{
				Err:                 errors.New("kafka.ProcessError"),
				DisableCircuitBreak: true,
			}
		},
	}

	cnt := atomic.Int64{}
	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))
	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		&kproc,
		zkafka.CircuitBreakAfter(1), // Circuit breaks after 1 error.
		zkafka.CircuitBreakFor(50*time.Millisecond),
		zkafka.WithOnDone(func(ctx context.Context, _ *zkafka.Message, _ error) {
			cnt.Add(1)
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return cnt.Load() >= 10
	}, pollOpts{
		exit: cancel,
		timeoutExit: func() {
			require.Failf(t, "Polling condition not met prior to test timeout", "Processing count %d", 10)
		},
	})

	require.LessOrEqual(t, time.Since(start), 50*time.Millisecond, "Every circuit breaker stoppage is 50ms, and we expect it to be skipped for each of the 10 failed messages. The expected time to process 10 messages is on the order of micro/nanoseconds, but we'll conservatively be happy with being less than a single circuit break cycle")
	require.NoError(t, grp.Wait())
}

func TestWork_Run_CircuitBreaksOnProcessPanicInsideProcessorGoRoutine(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key: ptr("1"),
		Fmt: &zfmt.JSONFormatter{},
	})
	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).AnyTimes().Return(msg, nil)

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).AnyTimes().Return(r, nil)

	kproc := &fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			panic("fake a panic occurring on process")
		},
	}

	cnt := atomic.Int64{}
	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))
	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		kproc,
		zkafka.CircuitBreakAfter(1), // Circuit breaks after 1 error.
		zkafka.CircuitBreakFor(50*time.Millisecond),
		zkafka.WithOnDone(func(ctx context.Context, _ *zkafka.Message, _ error) {
			cnt.Add(1)
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		ok := cnt.Load() >= 10
		if ok {
			cancel()
		}
		return ok
	}, pollOpts{
		timeoutExit: func() {
			require.Failf(t, "Polling condition not met prior to test timeout", "Processing count %d", 10)
		},
	})

	require.GreaterOrEqual(t, time.Since(start), 400*time.Millisecond, "Every circuit breaker stoppage is 50ms, and we expect it to be executed for each of the n failed messages with the exception of the first and second message (first trips, and second is read before the trip)")
	require.NoError(t, grp.Wait())
}

func TestWork_Run_DisabledCircuitBreakerContinueReadError(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka_mocks.NewMockLogger(ctrl)

	processingCount := 4
	l.EXPECT().Errorw(gomock.Any(), "Kafka topic single message processing failed", "error", gomock.Any(), "kmsg", gomock.Any()).AnyTimes()
	l.EXPECT().Warnw(gomock.Any(), "Kafka worker read message failed", "error", gomock.Any(), "topics", gomock.Any()).MinTimes(processingCount)
	l.EXPECT().Warnw(gomock.Any(), "Outside context canceled", "error", gomock.Any(), "kmsg", gomock.Any()).AnyTimes()
	l.EXPECT().Warnw(gomock.Any(), "Kafka topic processing circuit open", "topics", gomock.Any()).Times(0)
	l.EXPECT().Debugw(gomock.Any(), "Kafka topic message received", "offset", gomock.Any(), "partition", gomock.Any(), "topic", gomock.Any(), "groupID", gomock.Any()).AnyTimes()
	l.EXPECT().Debugw(gomock.Any(), gomock.Any()).AnyTimes()

	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).MinTimes(4).Return(nil, errors.New("error occurred on read"))

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(r, nil)

	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))

	cnt := atomic.Int64{}
	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		&fakeProcessor{},
		zkafka.DisableCircuitBreaker(),
		zkafka.WithLifecycleHooks(zkafka.LifecycleHooks{PostFanout: func(ctx context.Context) {
			cnt.Add(1)
		}}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		ok := cnt.Load() >= int64(processingCount)
		if ok {
			cancel()
		}
		return ok
	}, pollOpts{
		timeoutExit: func() {
			require.Failf(t, "Polling condition not met prior to test timeout", "Processing count %d", processingCount)
		},
	})
	require.NoError(t, grp.Wait())
}

func TestWork_Run_SpedUpIsFaster(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockReader := zkafka_mocks.NewMockReader(ctrl)

	mockReader.EXPECT().Read(gomock.Any()).DoAndReturn(func(ctx context.Context) (*zkafka.Message, error) {
		return zkafka.GetMsgFromFake(&zkafka.FakeMessage{
			Key: ptr(uuid.NewString()),
			Fmt: &zfmt.JSONFormatter{},
		}), nil

	}).AnyTimes()
	mockReader.EXPECT().Close().Return(nil).AnyTimes()

	mockClientProvider := zkafka_mocks.NewMockClientProvider(ctrl)
	mockClientProvider.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(2).Return(mockReader, nil)

	kwf := zkafka.NewWorkFactory(mockClientProvider, zkafka.WithLogger(zkafka.NoopLogger{}))
	slow := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			time.Sleep(time.Millisecond * 10)
			return nil
		},
	}
	fast := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			time.Sleep(time.Millisecond * 10)
			return nil
		},
	}

	func() {
		workerSlow := kwf.Create(
			zkafka.ConsumerTopicConfig{Topic: topicName},
			&slow,
		)

		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		require.NoError(t, workerSlow.Run(ctx, nil))
	}()

	// use te speedup option so more go routines process the read messages.
	// We'll let it process over the same amount of time (defined by timeout in context)
	func() {
		workerSpedUp := kwf.Create(
			zkafka.ConsumerTopicConfig{Topic: topicName},
			&fast,
			zkafka.Speedup(10),
		)

		ctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
		defer cancel()

		// wait for the cancel to occur via timeout
		require.NoError(t, workerSpedUp.Run(ctx, nil))
	}()

	// by putting a delay in the work.do method we minimize the comparative overhead in creating additional goroutines
	// and our speedup should begin to approach the KafkaSpeedup option of 10.
	// Because of hardware variance and context variance we'll only softly assert this speed up factor by asserting a range
	lowRangeSpeedup := 3
	highRangeSpeedup := 15
	slowCount := len(slow.ProcessedMessages())
	fastCount := len(fast.ProcessedMessages())
	lowerRange := slowCount * lowRangeSpeedup
	higherRange := slowCount * highRangeSpeedup

	if fastCount < lowerRange {
		t.Errorf("fast count should be at least %d times faster. fast count %d, slow count %d", lowRangeSpeedup, fastCount, slowCount)
	}
	if fastCount > higherRange {
		t.Errorf("fast count should have an upper limit on how much faster it is (no more than approximately %d faster). fast count %d, slow count %d", highRangeSpeedup, fastCount, slowCount)
	}
}

func TestKafkaWork_ProcessorReturnsErrorIsLoggedAsWarning(t *testing.T) {
	defer recoverThenFail(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	//
	l := zkafka_mocks.NewMockLogger(ctrl)
	l.EXPECT().Warnw(gomock.Any(), "Kafka topic processing circuit open", "topics", gomock.Any()).AnyTimes()
	l.EXPECT().Warnw(gomock.Any(), "Kafka topic single message processing failed", "error", gomock.Any(), "kmsg", gomock.Any()).MinTimes(1)
	l.EXPECT().Warnw(gomock.Any(), "Outside context canceled", "kmsg", gomock.Any(), "error", gomock.Any()).AnyTimes()
	l.EXPECT().Debugw(gomock.Any(), "Kafka topic message received", "offset", gomock.Any(), "partition", gomock.Any(), "topic", gomock.Any(), "groupID", gomock.Any()).AnyTimes()
	l.EXPECT().Debugw(gomock.Any(), gomock.Any()).AnyTimes()

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("key"),
		ValueData: "val",
		Fmt:       &zfmt.JSONFormatter{},
	})
	mockReader := zkafka_mocks.NewMockReader(ctrl)
	mockReader.EXPECT().Read(gomock.Any()).AnyTimes().Return(msg, nil)
	mockClientProvider := zkafka_mocks.NewMockClientProvider(ctrl)
	mockClientProvider.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(mockReader, nil)

	processor := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return errors.New("error for testcase TestKafkaWork_ProcessorReturnsErrorIsLoggedAsWarning")
		},
	}
	wf := zkafka.NewWorkFactory(mockClientProvider, zkafka.WithLogger(l))
	count := atomic.Int64{}
	work := wf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		&processor,
		zkafka.WithOnDone(func(ctx context.Context, message *zkafka.Message, err error) {
			count.Add(1)
		}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return work.Run(ctx, nil)
	})
	for {
		if count.Load() >= 1 {
			cancel()
			break
		}
		time.Sleep(time.Microsecond * 100)
	}
	require.NoError(t, grp.Wait())
}

// TestKafkaWork_ProcessorTimeoutCausesContextCancellation demonstrates that ProcessTimeoutMillis will
// cancel the context passed to the processor callback.
// The processor callback blocks until this context is cancelled, and then returns the error.
func TestKafkaWork_ProcessorTimeoutCausesContextCancellation(t *testing.T) {
	defer recoverThenFail(t)
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("key"),
		ValueData: "val",
		Fmt:       &zfmt.JSONFormatter{},
	})

	mockReader := zkafka_mocks.NewMockReader(ctrl)
	mockReader.EXPECT().Read(gomock.Any()).AnyTimes().Return(msg, nil)

	mockClientProvider := zkafka_mocks.NewMockClientProvider(ctrl)
	mockClientProvider.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(mockReader, nil)

	wf := zkafka.NewWorkFactory(mockClientProvider, zkafka.WithLogger(l))

	processor := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	count := atomic.Int64{}
	work := wf.Create(
		zkafka.ConsumerTopicConfig{
			Topic:                topicName,
			ProcessTimeoutMillis: ptr(1)},
		&processor,
		zkafka.WithOnDone(func(ctx context.Context, message *zkafka.Message, err error) {
			count.Add(1)
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return work.Run(ctx, nil)
	})
	for {
		if count.Load() >= 1 {
			cancel()
			break
		}
		time.Sleep(time.Microsecond * 100)
	}

	require.NoError(t, grp.Wait())
}

func TestWork_WithDeadLetterTopic_NoMessagesWrittenToDLTSinceNoErrorsOccurred(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	mockReader := zkafka_mocks.NewMockReader(ctrl)
	gomock.InOrder(
		mockReader.EXPECT().Read(gomock.Any()).Return(getRandomMessage(), nil),
		mockReader.EXPECT().Read(gomock.Any()).Return(getRandomMessage(), nil),
		mockReader.EXPECT().Read(gomock.Any()).Return(nil, nil).AnyTimes(),
	)
	mockReader.EXPECT().Close().Return(nil).AnyTimes()

	mockWriter := zkafka_mocks.NewMockWriter(ctrl)
	// no messages written into dlt because there weren't errors
	mockWriter.EXPECT().Write(gomock.Any(), gomock.Any()).Times(0)
	mockWriter.EXPECT().Close().AnyTimes()

	mockClientProvider := zkafka_mocks.NewMockClientProvider(ctrl)
	mockClientProvider.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(mockReader, nil)
	mockClientProvider.EXPECT().Writer(gomock.Any(), gomock.Any()).Times(2).Return(mockWriter, nil)

	kwf := zkafka.NewWorkFactory(mockClientProvider, zkafka.WithLogger(l))

	processor := fakeProcessor{}

	var cnt atomic.Int64
	w1 := kwf.Create(
		zkafka.ConsumerTopicConfig{
			Topic: topicName,
			DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
				ClientID: uuid.NewString(),
				Topic:    "topic2",
			},
		},
		&processor,
		zkafka.WithOnDone(func(ctx context.Context, message *zkafka.Message, err error) {
			cnt.Add(1)
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grp := errgroup.Group{}
	grp.Go(func() error {
		return w1.Run(ctx, nil)
	})

	pollWait(func() bool {
		stop := cnt.Load() == 2
		if stop {
			cancel()
		}
		return stop
	}, pollOpts{
		timeoutExit: func() {
			require.Fail(t, "Timed out during poll")
		},
		maxWait: 10 * time.Second,
	})

	require.NoError(t, grp.Wait())
}

func TestWork_WithDeadLetterTopic_MessagesWrittenToDLTSinceErrorOccurred(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	mockReader := zkafka_mocks.NewMockReader(ctrl)
	msg1 := getRandomMessage()
	msg2 := getRandomMessage()
	gomock.InOrder(
		mockReader.EXPECT().Read(gomock.Any()).Return(msg1, nil),
		mockReader.EXPECT().Read(gomock.Any()).Return(msg2, nil),
		mockReader.EXPECT().Read(gomock.Any()).Return(nil, nil).AnyTimes(),
	)
	mockReader.EXPECT().Close().Return(nil).AnyTimes()

	mockWriter := zkafka_mocks.NewMockWriter(ctrl)
	// each errored message gets forwarded
	mockWriter.EXPECT().WriteRaw(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Times(2)
	mockWriter.EXPECT().Close().AnyTimes()

	mockClientProvider := zkafka_mocks.NewMockClientProvider(ctrl)
	mockClientProvider.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(mockReader, nil)
	mockClientProvider.EXPECT().Writer(gomock.Any(), gomock.Any()).Times(2).Return(mockWriter, nil)

	kwf := zkafka.NewWorkFactory(mockClientProvider, zkafka.WithLogger(l))

	processor := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return errors.New("processor error")
		},
	}

	w1 := kwf.Create(
		zkafka.ConsumerTopicConfig{
			Topic: topicName,
			DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
				ClientID: uuid.NewString(),
				Topic:    "topic2",
			},
		},
		&processor,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w1.Run(ctx, nil)
	})

	pollWait(func() bool {
		return len(processor.ProcessedMessages()) == 2
	}, pollOpts{
		timeoutExit: func() {
			require.Fail(t, "Timed out during poll")
		},
		pollPause: time.Millisecond,
		maxWait:   10 * time.Second,
	})
	cancel()
	require.NoError(t, grp.Wait())
}

// TestWork_WithDeadLetterTopic_FailedToGetWriterDoesntPauseProcessing shows that even if get topic writer (for DLT) returns error processing still continues.
// This test configures a single virtual partition to process the reader. If processing halted on account of DLT write error,
// the test wouldn't get through all 10 messages
func TestWork_WithDeadLetterTopic_FailedToGetWriterDoesntPauseProcessing(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	mockReader := zkafka_mocks.NewMockReader(ctrl)
	msg1 := getRandomMessage()
	mockReader.EXPECT().Read(gomock.Any()).Times(10).Return(msg1, nil)
	mockReader.EXPECT().Read(gomock.Any()).Return(nil, nil).AnyTimes()
	mockReader.EXPECT().Close().Return(nil).AnyTimes()

	mockClientProvider := zkafka_mocks.NewMockClientProvider(ctrl)
	mockClientProvider.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(mockReader, nil)
	mockClientProvider.EXPECT().Writer(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, errors.New("failed to get dlt writer"))

	kwf := zkafka.NewWorkFactory(mockClientProvider, zkafka.WithLogger(l))

	processor := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return errors.New("processor error")
		},
	}

	dltTopic1 := "dlt-topic2"
	w1 := kwf.Create(
		zkafka.ConsumerTopicConfig{
			Topic:    topicName,
			ClientID: uuid.NewString(),
			GroupID:  uuid.NewString(),
			DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
				ClientID: uuid.NewString(),
				Topic:    dltTopic1,
			},
		},
		&processor,
		zkafka.DisableCircuitBreaker(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w1.Run(ctx, nil)
	})

	// the previous poll doesn't fully guarantee that the piece of code that
	pollWait(func() bool {
		return len(processor.ProcessedMessages()) == 10
	}, pollOpts{
		timeoutExit: func() {
			require.Failf(t, "Timed out during poll", "Processed Messages %d", len(processor.ProcessedMessages()))
		},
		pollPause: time.Millisecond,
		maxWait:   10 * time.Second,
	})
	cancel()
	require.NoError(t, grp.Wait())
}

// TestWork_WithDeadLetterTopic_FailedToWriteToDLTDoesntPauseProcessing even if callback can't write to DLT,  processing still continues.
// This test configures a single virtual partition to process the reader. If processing halted on account of DLT write error,
// the test wouldn't get through all 10 messages
func TestWork_WithDeadLetterTopic_FailedToWriteToDLTDoesntPauseProcessing(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	mockReader := zkafka_mocks.NewMockReader(ctrl)
	msg1 := getRandomMessage()
	mockReader.EXPECT().Read(gomock.Any()).Times(10).Return(msg1, nil)
	mockReader.EXPECT().Read(gomock.Any()).Return(nil, nil).AnyTimes()
	mockReader.EXPECT().Close().Return(nil).AnyTimes()

	mockWriter := zkafka_mocks.NewMockWriter(ctrl)
	mockWriter.EXPECT().WriteRaw(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(zkafka.Response{}, errors.New("error writing to dlt")).AnyTimes()
	mockWriter.EXPECT().Close().AnyTimes()

	mockClientProvider := zkafka_mocks.NewMockClientProvider(ctrl)
	mockClientProvider.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(mockReader, nil)
	mockClientProvider.EXPECT().Writer(gomock.Any(), gomock.Any()).AnyTimes().Return(mockWriter, nil)

	kwf := zkafka.NewWorkFactory(mockClientProvider, zkafka.WithLogger(l))

	processor := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return errors.New("processor error")
		},
	}

	dltTopic1 := "dlt-topic2"
	w1 := kwf.Create(
		zkafka.ConsumerTopicConfig{
			Topic:    topicName,
			ClientID: uuid.NewString(),
			GroupID:  uuid.NewString(),
			DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
				ClientID: uuid.NewString(),
				Topic:    dltTopic1,
			},
		},
		&processor,
		zkafka.DisableCircuitBreaker(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w1.Run(ctx, nil)
	})

	// the previous poll doesn't fully guarantee that the piece of code that
	pollWait(func() bool {
		return len(processor.ProcessedMessages()) == 10
	}, pollOpts{
		timeoutExit: func() {
			require.Failf(t, "Timed out during poll", "Processed Messages %d", len(processor.ProcessedMessages()))
		},
		pollPause: time.Millisecond,
		maxWait:   10 * time.Second,
	})
	cancel()
	require.NoError(t, grp.Wait())
}

func TestWork_DisableDLTWrite(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	mockReader := zkafka_mocks.NewMockReader(ctrl)
	msg1 := getRandomMessage()
	msg2 := getRandomMessage()
	gomock.InOrder(
		mockReader.EXPECT().Read(gomock.Any()).Return(msg1, nil),
		mockReader.EXPECT().Read(gomock.Any()).Return(msg2, nil),
		mockReader.EXPECT().Read(gomock.Any()).Return(nil, nil).AnyTimes(),
	)
	mockReader.EXPECT().Close().Return(nil).AnyTimes()

	mockWriter := zkafka_mocks.NewMockWriter(ctrl)
	// as we disabled the forwarding, we expect write to be called zero times
	mockWriter.EXPECT().WriteRaw(gomock.Any(), gomock.Any(), gomock.Any()).Times(0)
	mockWriter.EXPECT().Close().AnyTimes()

	mockClientProvider := zkafka_mocks.NewMockClientProvider(ctrl)
	mockClientProvider.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(mockReader, nil)
	mockClientProvider.EXPECT().Writer(gomock.Any(), gomock.Any()).Times(2).Return(mockWriter, nil)

	kwf := zkafka.NewWorkFactory(mockClientProvider, zkafka.WithLogger(l))

	processor := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return zkafka.ProcessError{
				Err:             errors.New("processor error"),
				DisableDLTWrite: true,
			}
		},
	}

	w1 := kwf.Create(
		zkafka.ConsumerTopicConfig{
			Topic: topicName,
			DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
				ClientID: uuid.NewString(),
				Topic:    "topic2",
			},
		},
		&processor,
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w1.Run(ctx, nil)
	})

	pollWait(func() bool {
		return len(processor.ProcessedMessages()) == 2
	}, pollOpts{
		timeoutExit: func() {
			require.Fail(t, "Timed out during poll")
		},
		pollPause: time.Millisecond,
		maxWait:   10 * time.Second,
	})
	cancel()
	require.NoError(t, grp.Wait())
}

// TestWork_Run_OnDoneCallbackCalledOnProcessorError asserts that our callback
// is called on processing error. It does this by registering a callback that will signal a channel when it's called.
// If there's a coding error
func TestWork_Run_OnDoneCallbackCalledOnProcessorError(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("1"),
		ValueData: "val",
		Fmt:       &zfmt.StringFormatter{},
	})

	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).AnyTimes().Return(msg, nil)

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(r, nil)

	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))

	errCount := atomic.Int64{}

	processingError := errors.New("failed processing")
	p := &fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return processingError
		},
	}
	var errReceived error
	errorCallback := func(ctx context.Context, _ *zkafka.Message, e error) {
		errReceived = e
		errCount.Add(1)
	}

	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		p,
		zkafka.WithOnDone(errorCallback),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})
	// wait until channel from error callback is written to
	pollWait(func() bool {
		return errCount.Load() >= 1
	}, pollOpts{
		exit:        cancel,
		timeoutExit: cancel,
	})

	require.ErrorIs(t, errReceived, processingError, "Expected processing error to be passed to callback")
	cancel()
	require.NoError(t, grp.Wait())
}

func TestWork_Run_WritesMetrics(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("key"),
		ValueData: "val",
		Fmt:       &zfmt.StringFormatter{},
	})

	msg.Topic = topicName
	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).MinTimes(1).Return(msg, nil)

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(r, nil)

	lhMtx := sync.Mutex{}
	lhState := FakeLifecycleState{
		numCalls: map[string]int{},
	}
	lh := NewFakeLifecycleHooks(&lhMtx, &lhState)
	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithWorkLifecycleHooks(lh))

	onDoneCount := atomic.Int64{}

	p := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return nil
		},
	}

	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName, GroupID: "xxx"},
		&p,
		zkafka.WithOnDone(func(ctx context.Context, _ *zkafka.Message, e error) { onDoneCount.Add(1) }),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})
	pollWait(func() bool {
		return onDoneCount.Load() >= 1
	}, pollOpts{
		exit:        cancel,
		timeoutExit: cancel,
	})
	require.NoError(t, grp.Wait())
}

func TestWork_LifecycleHooksCalledForEachItem_Reader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	numMsgs := 5
	msgs := getFakeMessages(topicName, numMsgs, struct{ name string }{name: "arish"}, &zfmt.JSONFormatter{})
	r := zkafka_mocks.NewMockReader(ctrl)

	gomock.InOrder(
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[0], nil),
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[1], nil),
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[2], nil),
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[3], nil),
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[4], nil),
		r.EXPECT().Read(gomock.Any()).AnyTimes().Return(nil, nil),
	)

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(r, nil)

	lhMtx := sync.Mutex{}
	lhState := FakeLifecycleState{
		numCalls: map[string]int{},
	}
	lm := NewFakeLifecycleHooks(&lhMtx, &lhState)
	wf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l), zkafka.WithWorkLifecycleHooks(lm))
	p := fakeProcessor{}

	var numProcessedItems int32
	w := wf.Create(zkafka.ConsumerTopicConfig{Topic: topicName, GroupID: "xxx"},
		&p,
		zkafka.WithOnDone(func(ctx context.Context, msg *zkafka.Message, err error) {
			atomic.AddInt32(&numProcessedItems, 1)
		}))

	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return int(atomic.LoadInt32(&numProcessedItems)) == numMsgs
	}, pollOpts{
		exit: cancel,
	})

	require.Equal(t, numMsgs, int(atomic.LoadInt32(&numProcessedItems)))

	require.Len(t, lhState.preProMeta, numMsgs)
	require.Len(t, lhState.postProMeta, numMsgs)
	require.Len(t, lhState.preReadMeta, numMsgs)
	require.Equal(t, 0, len(lhState.postAckMeta))

	require.Equal(t, lhState.preProMeta[0].Topic, topicName)
	require.Equal(t, lhState.preProMeta[0].GroupID, "xxx")
	require.Equal(t, lhState.preProMeta[0].VirtualPartitionIndex, 0)

	require.Equal(t, lhState.postProMeta[0].Topic, topicName)
	require.Equal(t, lhState.postProMeta[0].GroupID, "xxx")
	require.Equal(t, lhState.postProMeta[0].VirtualPartitionIndex, 0)
	require.NoError(t, grp.Wait())
}

func TestWork_LifecycleHooksPostReadCanUpdateContext(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	numMsgs := 1
	msgs := getFakeMessages(topicName, numMsgs, "lydia", &zfmt.JSONFormatter{})
	r := zkafka_mocks.NewMockReader(ctrl)

	gomock.InOrder(
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[0], nil),
		r.EXPECT().Read(gomock.Any()).AnyTimes().Return(nil, nil),
	)

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(r, nil)

	lhMtx := sync.Mutex{}
	lhState := FakeLifecycleState{
		numCalls: map[string]int{},
	}
	lm := NewFakeLifecycleHooks(&lhMtx, &lhState)
	lm.PostRead = func(ctx context.Context, meta zkafka.LifecyclePostReadMeta) (context.Context, error) {
		return context.WithValue(ctx, "stewy", "hello"), nil
	}
	wf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l), zkafka.WithWorkLifecycleHooks(lm))
	var capturedContext context.Context
	p := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			capturedContext = ctx
			return nil
		},
	}

	var numProcessedItems int32
	w := wf.Create(zkafka.ConsumerTopicConfig{Topic: topicName, GroupID: "xxx"},
		&p,
		zkafka.WithOnDone(func(ctx context.Context, msg *zkafka.Message, err error) {
			atomic.AddInt32(&numProcessedItems, 1)
		}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return int(atomic.LoadInt32(&numProcessedItems)) == numMsgs
	}, pollOpts{
		exit: cancel,
	})

	require.Equal(t, capturedContext.Value("stewy"), "hello", "Expect context passed to process to include data injected at post read step")
	require.NoError(t, grp.Wait())
}

func TestWork_LifecycleHooksPostReadErrorDoesntHaltProcessing(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	numMsgs := 1
	msgs := getFakeMessages(topicName, numMsgs, "lydia", &zfmt.JSONFormatter{})
	r := zkafka_mocks.NewMockReader(ctrl)

	gomock.InOrder(
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[0], nil),
		r.EXPECT().Read(gomock.Any()).AnyTimes().Return(nil, nil),
	)

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(r, nil)

	lhMtx := sync.Mutex{}
	lhState := FakeLifecycleState{
		numCalls: map[string]int{},
	}
	lm := NewFakeLifecycleHooks(&lhMtx, &lhState)
	lm.PostRead = func(ctx context.Context, meta zkafka.LifecyclePostReadMeta) (context.Context, error) {
		return ctx, errors.New("post read hook error")
	}
	wf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l), zkafka.WithWorkLifecycleHooks(lm))
	p := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return nil
		},
	}

	var numProcessedItems int32
	w := wf.Create(zkafka.ConsumerTopicConfig{Topic: topicName, GroupID: "xxx"},
		&p,
		zkafka.WithOnDone(func(ctx context.Context, msg *zkafka.Message, err error) {
			atomic.AddInt32(&numProcessedItems, 1)
		}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return int(atomic.LoadInt32(&numProcessedItems)) == numMsgs
	}, pollOpts{
		exit: cancel,
	})
	require.NoError(t, grp.Wait())
}

func TestWork_LifecycleHooksCalledForEachItem(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}
	numMsgs := 5
	msgs := getFakeMessages(topicName, numMsgs, struct{ name string }{name: "arish"}, &zfmt.JSONFormatter{})
	r := zkafka_mocks.NewMockReader(ctrl)

	gomock.InOrder(
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[0], nil),
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[1], nil),
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[2], nil),
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[3], nil),
		r.EXPECT().Read(gomock.Any()).Times(1).Return(msgs[4], nil),
		r.EXPECT().Read(gomock.Any()).AnyTimes().Return(nil, nil),
	)

	qp := zkafka_mocks.NewMockClientProvider(ctrl)
	qp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(r, nil)

	lhMtx := sync.Mutex{}
	lhState := FakeLifecycleState{
		numCalls: map[string]int{},
	}
	lh := NewFakeLifecycleHooks(&lhMtx, &lhState)
	wf := zkafka.NewWorkFactory(qp, zkafka.WithLogger(l), zkafka.WithWorkLifecycleHooks(lh))
	p := fakeProcessor{}

	var numProcessedItems int32
	w := wf.Create(zkafka.ConsumerTopicConfig{Topic: topicName, GroupID: "xxx"},
		&p, zkafka.WithOnDone(func(ctx context.Context, msg *zkafka.Message, err error) {
			atomic.AddInt32(&numProcessedItems, 1)
		}))

	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return int(atomic.LoadInt32(&numProcessedItems)) == numMsgs
	}, pollOpts{
		exit: cancel,
	})

	require.Equal(t, numMsgs, lhState.numCalls["pre-processing"])
	require.Equal(t, numMsgs, lhState.numCalls["post-processing"])
	require.Equal(t, 0, lhState.numCalls["post-ack"])
	require.NoError(t, grp.Wait())
}

type FakeLifecycleState struct {
	numCalls     map[string]int
	preProMeta   []zkafka.LifecyclePreProcessingMeta
	postProMeta  []zkafka.LifecyclePostProcessingMeta
	postAckMeta  []zkafka.LifecyclePostAckMeta
	preReadMeta  []zkafka.LifecyclePostReadMeta
	preWriteMeta []zkafka.LifecyclePreWriteMeta
}

func NewFakeLifecycleHooks(mtx *sync.Mutex, state *FakeLifecycleState) zkafka.LifecycleHooks {
	h := zkafka.LifecycleHooks{
		PostRead: func(ctx context.Context, meta zkafka.LifecyclePostReadMeta) (context.Context, error) {
			mtx.Lock()
			state.numCalls["pre-read"] += 1
			state.preReadMeta = append(state.preReadMeta, meta)
			mtx.Unlock()
			return ctx, nil
		},
		PreProcessing: func(ctx context.Context, meta zkafka.LifecyclePreProcessingMeta) (context.Context, error) {
			mtx.Lock()
			state.numCalls["pre-processing"] += 1
			state.preProMeta = append(state.preProMeta, meta)
			mtx.Unlock()
			return ctx, nil
		},
		PostProcessing: func(ctx context.Context, meta zkafka.LifecyclePostProcessingMeta) error {
			mtx.Lock()
			state.numCalls["post-processing"] += 1
			state.postProMeta = append(state.postProMeta, meta)
			mtx.Unlock()
			return nil
		},
		PostAck: func(ctx context.Context, meta zkafka.LifecyclePostAckMeta) error {
			mtx.Lock()
			state.numCalls["post-ack"] += 1
			state.postAckMeta = append(state.postAckMeta, meta)
			mtx.Unlock()
			return nil
		},
		PreWrite: func(ctx context.Context, meta zkafka.LifecyclePreWriteMeta) (zkafka.LifecyclePreWriteResp, error) {
			mtx.Lock()
			state.numCalls["pre-write"] += 1
			state.preWriteMeta = append(state.preWriteMeta, meta)
			mtx.Unlock()
			return zkafka.LifecyclePreWriteResp{}, nil
		},
	}

	return h
}

func getRandomMessage() *zkafka.Message {
	return zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:      ptr(fmt.Sprintf("%d", rand.Intn(5))),
		DoneFunc: func(ctx context.Context) {},
		Fmt:      &zfmt.JSONFormatter{},
	})
}

func TestWork_CircuitBreaker_WithoutBusyLoopBreaker_DoesNotWaitsForCircuitToOpen(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("1"),
		ValueData: struct{ name string }{name: "arish"},
		Fmt:       &zfmt.JSONFormatter{},
	})
	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).Return(msg, nil).AnyTimes()

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Return(r, nil).AnyTimes()

	//l := zkafka.NoopLogger{}
	l := stdLogger{includeDebug: true}
	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))

	fanOutCount := atomic.Int64{}
	processorCount := atomic.Int64{}
	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		&fakeProcessor{
			process: func(ctx context.Context, message *zkafka.Message) error {
				processorCount.Add(1)
				return errors.New("an error occurred during processing")
			},
		},
		zkafka.DisableBusyLoopBreaker(),
		zkafka.WithLifecycleHooks(zkafka.LifecycleHooks{PostFanout: func(ctx context.Context) {
			fanOutCount.Add(1)
		}}),
		zkafka.CircuitBreakAfter(1),
		zkafka.CircuitBreakFor(10*time.Second),
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	start := time.Now()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return fanOutCount.Load() >= 100
	}, pollOpts{
		exit: cancel,
		timeoutExit: func() {
			require.Failf(t, "Timed out during poll", "Fanout Count %d", fanOutCount.Load())
		},
		maxWait: 10 * time.Second,
	})
	require.LessOrEqual(t, processorCount.Load(), int64(2), "circuit breaker should prevent processor from being called after circuit break opens, since circuit breaker won't close again until after test completes. At most two messages are read prior to circuit breaker opening")
	require.LessOrEqual(t, time.Since(start), time.Second, "without busy loop breaker we expect fanOut to called rapidly. Circuit break is open for 10 seconds. So asserting that fanOut was called 100 times in a second is a rough assertion that busy loop breaker is not in effect. Typically these 100 calls should be on the order of micro or nanoseconds. But with resource contention in the pipeline we're more conservative with timing based assertions")
	t.Log("begin")
	cancel()
	t.Log("nend")
	require.NoError(t, grp.Wait())
}

func TestWork_CircuitBreaker_WaitsForCircuitToOpen(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("1"),
		ValueData: struct{ name string }{name: "arish"},
		Fmt:       &zfmt.JSONFormatter{},
	})

	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).Return(msg, nil).AnyTimes()

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Return(r, nil).AnyTimes()

	kwf := zkafka.NewWorkFactory(kcp)

	processCount := atomic.Int64{}
	circuitBreakDuration := 10 * time.Millisecond
	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		&fakeProcessor{
			process: func(ctx context.Context, message *zkafka.Message) error {
				processCount.Add(1)
				return errors.New("an error occurred during processing")
			},
		},
		zkafka.CircuitBreakAfter(1),
		zkafka.CircuitBreakFor(circuitBreakDuration),
	)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	start := time.Now()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	loopCount := int64(5)
	for {
		if processCount.Load() == loopCount {
			cancel()
			break
		}
		time.Sleep(time.Microsecond * 100)
	}
	require.GreaterOrEqual(t, circuitBreakDuration*time.Duration(loopCount), time.Since(start), "Total time should be greater than circuit break duration * loop count")
	require.NoError(t, grp.Wait())
}

// TestWork_DontDeadlockWhenCircuitBreakerIsInHalfOpen this test protects against a bug that was demonstrated in another worker library which implements similar behavior.
// Because of this, this test was written to protect against a regression similar to what was observed in that lib.
//
// This test aims to get the worker into a half open state (by returning processor errors) with short circuit breaker times.
// This test asserts we can process 10 messages in less than 10 seconds (should be able to process in about 1), and assumes
// if we can't a deadlock has occurred
func TestWork_DontDeadlockWhenCircuitBreakerIsInHalfOpen(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	qr := zkafka_mocks.NewMockReader(ctrl)
	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("1"),
		ValueData: struct{ name string }{name: "stewy"},
		Fmt:       &zfmt.JSONFormatter{},
	})
	gomock.InOrder(
		qr.EXPECT().Read(gomock.Any()).Times(1).Return(msg, nil),
		qr.EXPECT().Read(gomock.Any()).AnyTimes().Return(nil, nil),
	)

	cp := zkafka_mocks.NewMockClientProvider(ctrl)
	cp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(qr, nil)

	wf := zkafka.NewWorkFactory(cp)

	p := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			return errors.New("an error occurred during processing")
		},
	}

	fanOutCount := atomic.Int64{}
	w := wf.Create(zkafka.ConsumerTopicConfig{Topic: topicName},
		&p,
		// go into half state almost immediately after processing the message.
		zkafka.CircuitBreakFor(time.Microsecond),
		// update so we enter open state immediately once one processing error occurs
		zkafka.CircuitBreakAfter(1),
		zkafka.WithLifecycleHooks(zkafka.LifecycleHooks{PostFanout: func(ctx context.Context) {
			time.Sleep(time.Millisecond * 100)
			fanOutCount.Add(1)
		}}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	start := time.Now()
	for {
		// if we don't hit a deadlock we should get to 10 loops of Do execution quickly (especially since there's no messages to process after subsequent read)
		if fanOutCount.Load() >= 10 {
			cancel()
			break
		}
		// take small breaks while polling
		time.Sleep(time.Microsecond)
		require.GreaterOrEqual(t, 10*time.Second, time.Since(start), "Process timeout likely not being respected. Likely entered a deadlock due to circuit breaker")
	}
	require.NoError(t, grp.Wait())
}

// Test_Bugfix_WorkPoolCanBeRestartedAfterShutdown this test is in response to a bug
// that occurs during testing. The initial implementation of work (specifically a deprecated Do method) only allowed the worker pool to be started once.
// When the worker was torn down (by cancelling the context), it was unable to be restarted.
// This is primarily a vestigial concern, since the implementation starts a worker pool everytime run is called (instead of having to start it and then stop it and then potentially restart like Do)
// Some test patterns, were relying on the same work instance to be started and stopped multiple times across multiple tests.
func Test_Bugfix_WorkPoolCanBeRestartedAfterShutdown(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	mockReader := zkafka_mocks.NewMockReader(ctrl)
	msg1 := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("abc"),
		ValueData: "def",
		Fmt:       &zfmt.StringFormatter{},
	})
	mockReader.EXPECT().Read(gomock.Any()).Return(msg1, nil).AnyTimes()
	mockReader.EXPECT().Close().Return(nil).AnyTimes()

	mockClientProvider := zkafka_mocks.NewMockClientProvider(ctrl)
	mockClientProvider.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(mockReader, nil)
	mockClientProvider.EXPECT().Writer(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)

	kwf := zkafka.NewWorkFactory(mockClientProvider, zkafka.WithLogger(l))

	processor := fakeProcessor{}

	w := kwf.Create(
		zkafka.ConsumerTopicConfig{
			Topic: topicName,
			DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
				ClientID: uuid.NewString(),
				Topic:    "topic2",
			},
		},
		&processor,
	)

	t.Log("Starting first work.Run")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg := errgroup.Group{}
	wg.Go(func() error {
		return w.Run(context.Background(), ctx.Done())
	})

	// wait for at least 1 message to be processed and then cancel the context (which will stop worker)
	// and break for loop
	for {
		if len(processor.ProcessedMessages()) >= 1 {
			cancel()
			break
		}
		time.Sleep(time.Millisecond)
	}
	// wait until worker fully completes and returns
	require.NoError(t, wg.Wait())
	t.Log("Completed first work.Run")

	// take a count of how many messages were processed. Because of concurrent processing it might be more than 1
	startCount := len(processor.ProcessedMessages())

	// Start the worker again (make sure you don't pass in the canceled context).
	ctx2, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(context.Background(), ctx2.Done())
	})

	t.Log("Started polling for second work.Run")

	// This is the assertion portion of the test. We're asserting the processing will continue
	// and then message count will increase beyond what was originally counted.
	// If we exit the test was a success. A bug will indefinitely block
	pollWait(func() bool {
		return len(processor.ProcessedMessages()) > startCount
	}, pollOpts{
		exit: cancel,
		timeoutExit: func() {
			require.Failf(t, "Polling condition not met prior to test timeout", "Processing count %d, startcount %d", len(processor.ProcessedMessages()), startCount)
		},
		maxWait: 10 * time.Second,
	})
}

// Test_MsgOrderingIsMaintainedPerKeyWithAnyNumberOfVirtualPartitions
// given N messages ordered as follows [{key=0,val=0}, {key=1,val=0}, {key=2,val=0}, {key=0,val=1}, {key=1,val=1}, {key=2,val=1}, ... {key=0,val=N}, {key=1,val=N}, {key=2,val=N}]
// when a work is created with speedup
// then those messages are processed in order per key. To assert this, we track all the messages processed
// and then assert that the value is increasing per key (0, 1 and 2)
func Test_MsgOrderingIsMaintainedPerKeyWithAnyNumberOfVirtualPartitions(t *testing.T) {
	defer recoverThenFail(t)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	mockReader := zkafka_mocks.NewMockReader(ctrl)
	var readerCalls []*gomock.Call
	keyCount := 3
	msgCount := 200
	for i := 0; i < msgCount; i++ {
		msg1 := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
			Key:       ptr(strconv.Itoa(i % keyCount)),
			ValueData: strconv.Itoa(i),
			Fmt:       &zfmt.StringFormatter{},
		})
		readerCalls = append(readerCalls, mockReader.EXPECT().Read(gomock.Any()).Return(msg1, nil))
	}
	readerCalls = append(readerCalls, mockReader.EXPECT().Read(gomock.Any()).Return(nil, nil).AnyTimes())
	gomock.InOrder(
		readerCalls...,
	)
	mockReader.EXPECT().Close().Return(nil).AnyTimes()

	mockClientProvider := zkafka_mocks.NewMockClientProvider(ctrl)
	mockClientProvider.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(mockReader, nil)
	mockClientProvider.EXPECT().Writer(gomock.Any(), gomock.Any()).AnyTimes().Return(nil, nil)

	kwf := zkafka.NewWorkFactory(mockClientProvider, zkafka.WithLogger(l))

	processor := fakeProcessor{
		process: func(ctx context.Context, message *zkafka.Message) error {
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
			return nil
		},
	}

	w := kwf.Create(
		zkafka.ConsumerTopicConfig{
			Topic: topicName,
			DeadLetterTopicConfig: &zkafka.ProducerTopicConfig{
				ClientID: uuid.NewString(),
				Topic:    "topic2",
			},
		},
		&processor,
		zkafka.Speedup(10),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})

	pollWait(func() bool {
		return len(processor.ProcessedMessages()) == msgCount
	}, pollOpts{
		exit: cancel,
	})

	keyToMsgs := make(map[string][]*zkafka.Message)
	for _, m := range processor.ProcessedMessages() {
		keyToMsgs[m.Key] = append(keyToMsgs[m.Key], m)
	}
	vals0 := make([]int, 0, len(keyToMsgs["0"]))
	for _, m := range keyToMsgs["0"] {
		i, err := strconv.Atoi(string(m.Value()))
		require.NoError(t, err)
		vals0 = append(vals0, i)
	}
	vals1 := make([]int, 0, len(keyToMsgs["1"]))
	for _, m := range keyToMsgs["1"] {
		i, err := strconv.Atoi(string(m.Value()))
		require.NoError(t, err)
		vals1 = append(vals1, i)
	}
	vals2 := make([]int, 0, len(keyToMsgs["2"]))
	for _, m := range keyToMsgs["2"] {
		i, err := strconv.Atoi(string(m.Value()))
		require.NoError(t, err)
		vals2 = append(vals2, i)
	}
	require.IsIncreasingf(t, vals0, "messages for key 0 are not sorted %v", vals0)
	require.IsIncreasingf(t, vals1, "messages for key 1 are not sorted")
	require.IsIncreasingf(t, vals2, "messages for key 2 are not sorted")
	require.NoError(t, grp.Wait())
}

func TestWork_LifecycleHookReaderPanicIsHandledAndMessagingProceeds(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	l := zkafka.NoopLogger{}

	testPanic := func(hooks zkafka.LifecycleHooks) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		qr := zkafka_mocks.NewMockReader(ctrl)
		numMsgs := 1
		sentMsg := false
		msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
			Key:       ptr("1"),
			ValueData: struct{ name string }{name: "arish"},
			Fmt:       &zfmt.JSONFormatter{},
		})

		qr.EXPECT().Read(gomock.Any()).AnyTimes().DoAndReturn(func(ctx context.Context) (*zkafka.Message, error) {
			if !sentMsg {
				sentMsg = true
				return msg, nil
			}
			return nil, nil
		})

		qp := zkafka_mocks.NewMockClientProvider(ctrl)
		qp.EXPECT().Reader(gomock.Any(), gomock.Any()).Times(1).Return(qr, nil)

		wf := zkafka.NewWorkFactory(qp, zkafka.WithLogger(l), zkafka.WithWorkLifecycleHooks(hooks))

		p := fakeProcessor{
			process: func(ctx context.Context, message *zkafka.Message) error {
				return nil
			},
		}

		m := sync.Mutex{}
		var processedMsgs []*zkafka.Message
		topicConfig := zkafka.ConsumerTopicConfig{
			ClientID:  "test-config",
			GroupID:   "group",
			Topic:     "topic",
			Formatter: zfmt.JSONFmt,
		}
		w := wf.Create(topicConfig, &p,
			zkafka.WithOnDone(func(ctx context.Context, msg *zkafka.Message, err error) {
				m.Lock()
				processedMsgs = append(processedMsgs, msg)
				m.Unlock()
			}),
		)

		grp := errgroup.Group{}
		grp.Go(func() error {
			return w.Run(ctx, nil)
		})

		for {
			m.Lock()
			msgCount := len(processedMsgs)
			m.Unlock()

			if msgCount == numMsgs {
				cancel()
				break
			}
		}

		require.Len(t, processedMsgs, numMsgs)
		require.NoError(t, grp.Wait())
	}

	testPanic(zkafka.LifecycleHooks{
		PreProcessing: func(ctx context.Context, meta zkafka.LifecyclePreProcessingMeta) (context.Context, error) {
			panic("pre processing panic")
		},
	})
	testPanic(zkafka.LifecycleHooks{
		PostProcessing: func(ctx context.Context, meta zkafka.LifecyclePostProcessingMeta) error {
			panic("post processing panic")
		},
	})
}

func TestWork_ShutdownCausesRunExit(t *testing.T) {
	defer recoverThenFail(t)
	ctx := context.Background()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("1"),
		ValueData: struct{ name string }{name: "arish"},
		Fmt:       &zfmt.JSONFormatter{},
	})

	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).Return(msg, nil).AnyTimes()

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Return(r, nil).AnyTimes()

	l := zkafka.NoopLogger{}
	kwf := zkafka.NewWorkFactory(kcp, zkafka.WithLogger(l))

	fanOutCount := atomic.Int64{}
	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		&fakeProcessor{},
		zkafka.WithLifecycleHooks(zkafka.LifecycleHooks{PostFanout: func(ctx context.Context) {
			fanOutCount.Add(1)
		}}),
	)

	settings := &workSettings{
		shutdownSig: make(chan struct{}, 1),
	}
	go func() {
		pollWait(func() bool {
			return fanOutCount.Load() >= 1
		}, pollOpts{
			maxWait: 10 * time.Second,
		})
		close(settings.shutdownSig)
	}()

	err := w.Run(ctx, settings.ShutdownSig())
	require.NoError(t, err)
}

// $ go test -run=XXX -bench=BenchmarkWork_Run_CircuitBreaker_BusyLoopBreaker -cpuprofile profile_cpu.out
// $ go tool pprof --web profile_cpu.out
// $ go tool pprof -http=":8000" test.test ./profile_cpu.out
func BenchmarkWork_Run_CircuitBreaker_BusyLoopBreaker(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()

	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("1"),
		ValueData: struct{ name string }{name: "arish"},
		Fmt:       &zfmt.JSONFormatter{},
	})
	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).Return(msg, nil).AnyTimes()

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Return(r, nil).AnyTimes()

	kwf := zkafka.NewWorkFactory(kcp)

	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		&fakeProcessor{
			process: func(ctx context.Context, message *zkafka.Message) error {
				return errors.New("an error occurred during processing")
			},
		},
		zkafka.Speedup(10),
		zkafka.CircuitBreakAfter(100),
		zkafka.CircuitBreakFor(30*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})
	require.NoError(b, grp.Wait())
}

// $ go test -run=XXX -bench=BenchmarkWork_Run_CircuitBreaker_DisableBusyLoopBreaker -cpuprofile profile_cpu_disable.out
// $ go tool pprof --web profile_cpu_disable.out
// $go tool pprof -http=":8000" test.test ./profile_cpu_disable.out
func BenchmarkWork_Run_CircuitBreaker_DisableBusyLoopBreaker(b *testing.B) {
	b.ReportAllocs()
	ctx := context.Background()

	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	msg := zkafka.GetMsgFromFake(&zkafka.FakeMessage{
		Key:       ptr("1"),
		ValueData: struct{ name string }{name: "arish"},
		Fmt:       &zfmt.JSONFormatter{},
	})
	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).Return(msg, nil).AnyTimes()

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Return(r, nil).AnyTimes()

	kwf := zkafka.NewWorkFactory(kcp)

	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: topicName},
		&fakeProcessor{
			process: func(ctx context.Context, message *zkafka.Message) error {
				return errors.New("an error occurred during processing")
			},
		},
		zkafka.Speedup(10),
		zkafka.CircuitBreakAfter(100),
		zkafka.CircuitBreakFor(30*time.Millisecond),
		zkafka.DisableBusyLoopBreaker(),
	)

	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	grp := errgroup.Group{}
	grp.Go(func() error {
		return w.Run(ctx, nil)
	})
	require.NoError(b, grp.Wait())
}

func recoverThenFail(t *testing.T) {
	if r := recover(); r != nil {
		fmt.Print(string(debug.Stack()))
		t.Fatal(r)
	}
}

type fakeProcessor struct {
	m                 sync.Mutex
	processedMessages []*zkafka.Message
	processedContexts []context.Context
	process           func(context.Context, *zkafka.Message) error
}

func (p *fakeProcessor) Process(ctx context.Context, msg *zkafka.Message) error {
	p.m.Lock()
	p.processedMessages = append(p.processedMessages, msg)
	p.processedContexts = append(p.processedContexts, ctx)
	p.m.Unlock()
	if p.process != nil {
		return p.process(ctx, msg)
	}
	return nil
}

func (p *fakeProcessor) ProcessedMessages() []*zkafka.Message {
	p.m.Lock()
	defer p.m.Unlock()

	var msgs []*zkafka.Message
	for _, m := range p.processedMessages {
		msgs = append(msgs, m)
	}
	return msgs
}

func (p *fakeProcessor) ProcessedContexts() []context.Context {
	p.m.Lock()
	defer p.m.Unlock()

	var ctxs []context.Context
	for _, ctx := range p.processedContexts {
		ctxs = append(ctxs, ctx)
	}
	return ctxs
}

type stdLogger struct {
	includeDebug bool
}

func (l stdLogger) Debugw(_ context.Context, msg string, keysAndValues ...interface{}) {
	if l.includeDebug {
		log.Printf("Debugw-"+msg, keysAndValues...)
	}
}

func (l stdLogger) Infow(_ context.Context, msg string, keysAndValues ...interface{}) {
	log.Printf("Infow-"+msg, keysAndValues...)
}

func (l stdLogger) Errorw(_ context.Context, msg string, keysAndValues ...interface{}) {
	log.Printf("Errorw-"+msg, keysAndValues...)
}

func (l stdLogger) Warnw(_ context.Context, msg string, keysAndValues ...interface{}) {
	prefix := fmt.Sprintf("Warnw-%s-"+msg, time.Now().Format(time.RFC3339Nano))
	log.Printf(prefix, keysAndValues...)
}

type workSettings struct {
	shutdownSig chan struct{}
}

func (w *workSettings) ShutdownSig() <-chan struct{} {
	return w.shutdownSig
}

func ptr[T any](v T) *T {
	return &v
}

type pollOpts struct {
	exit        func()
	timeoutExit func()
	pollPause   time.Duration
	maxWait     time.Duration
}

func pollWait(f func() bool, opts pollOpts) {
	maxWait := time.Minute
	pollPause := time.Millisecond

	if opts.pollPause != 0 {
		pollPause = opts.pollPause
	}
	if opts.maxWait != 0 {
		maxWait = opts.maxWait
	}

	start := time.Now()
	for {
		if f() {
			if opts.exit != nil {
				opts.exit()
			}
			return
		}
		if time.Since(start) > maxWait {
			if opts.timeoutExit != nil {
				opts.timeoutExit()
			}
			break
		}
		time.Sleep(pollPause)
	}
}

func getFakeMessages(topic string, numMsgs int, value any, formatter zfmt.Formatter) []*zkafka.Message {
	msgs := make([]*zkafka.Message, numMsgs)

	for i := 0; i < numMsgs; i++ {
		key := fmt.Sprint(i)
		msgs[i] = zkafka.GetMsgFromFake(&zkafka.FakeMessage{
			Key:       &key,
			ValueData: value,
			Fmt:       formatter,
		})
		msgs[i].Topic = topic
	}

	return msgs
}
