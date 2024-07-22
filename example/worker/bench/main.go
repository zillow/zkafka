package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"runtime/pprof"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/zillow/zkafka"
	zkafka_mocks "github.com/zillow/zkafka/mocks"
	"gitlab.zgtools.net/devex/archetypes/gomods/zfmt"
)

func main() {
	f, err := os.Create("cpu.prof")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close() // error handling omitted for example
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	ctrl := gomock.NewController(&testReporter{})
	defer ctrl.Finish()
	messageDone := func() {
	}
	msg := zkafka.GetFakeMessage("1", struct{ name string }{name: "arish"}, &zfmt.JSONFormatter{}, messageDone)
	r := zkafka_mocks.NewMockReader(ctrl)
	r.EXPECT().Read(gomock.Any()).Return(msg, nil).AnyTimes()

	kcp := zkafka_mocks.NewMockClientProvider(ctrl)
	kcp.EXPECT().Reader(gomock.Any(), gomock.Any()).Return(r, nil).AnyTimes()

	kwf := zkafka.NewWorkFactory(kcp)
	w := kwf.Create(
		zkafka.ConsumerTopicConfig{Topic: "topicName"},
		&kafkaProcessorError{},
		zkafka.Speedup(10),
		zkafka.CircuitBreakAfter(100),
		zkafka.CircuitBreakFor(30*time.Second),
		zkafka.DisableBusyLoopBreaker(),
	)
	ctx, c := context.WithTimeout(context.Background(), 2*time.Minute)
	defer c()
	w.Run(ctx, nil)
}

type kafkaProcessorError struct{}

func (p *kafkaProcessorError) Process(_ context.Context, _ *zkafka.Message) error {
	fmt.Print(".")
	return errors.New("an error occurred during processing")
}

type testReporter struct{}

func (t *testReporter) Errorf(format string, args ...any) {}
func (t *testReporter) Fatalf(format string, args ...any) {}
