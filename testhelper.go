package zkafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gitlab.zgtools.net/devex/archetypes/gomods/zfmt"
)

func GetFakeMessage(key string, value any, fmt zfmt.Formatter, doneFunc func()) *Message {
	wrapperFunc := func(c context.Context) { doneFunc() }
	return GetFakeMessageWithContext(key, value, fmt, wrapperFunc)
}

func GetFakeMessages(topic string, numMsgs int, value any, formatter zfmt.Formatter, doneFunc func()) []*Message {
	msgs := make([]*Message, numMsgs)
	wrapperFunc := func(c context.Context) { doneFunc() }

	for i := 0; i < numMsgs; i++ {
		key := fmt.Sprint(i)
		msgs[i] = GetFakeMessageWithContext(key, value, formatter, wrapperFunc)
		msgs[i].Topic = topic
	}

	return msgs
}

func GetFakeMessageWithContext(key string, value any, fmt zfmt.Formatter, doneFunc func(context.Context)) *Message {
	if b, err := fmt.Marshall(value); err == nil {
		return &Message{
			Key:       key,
			Headers:   nil,
			value:     b,
			fmt:       fmt,
			doneFunc:  doneFunc,
			doneOnce:  sync.Once{},
			TimeStamp: time.Now(),
		}
	}
	return &Message{
		Key:       key,
		doneFunc:  doneFunc,
		doneOnce:  sync.Once{},
		TimeStamp: time.Now(),
	}
}

func GetFakeMsgFromRaw(key *string, value []byte, fmt zfmt.Formatter, doneFunc func(context.Context)) *Message {
	k := ""
	if key != nil {
		k = *key
	}
	return &Message{
		Key:       k,
		Headers:   nil,
		value:     value,
		fmt:       fmt,
		doneFunc:  doneFunc,
		doneOnce:  sync.Once{},
		TimeStamp: time.Now(),
	}
}
