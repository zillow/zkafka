package zstreams

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/propagation"
)

var _ propagation.TextMapCarrier = (*kMsgCarrier)(nil)
var _ propagation.TextMapCarrier = (*msgCarrier)(nil)

type kMsgCarrier struct {
	msg *kafka.Message
}

func (c *kMsgCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c *kMsgCarrier) Keys() []string {
	keys := make([]string, 0, len(c.msg.Headers))
	for _, v := range c.msg.Headers {
		keys = append(keys, v.Key)
	}
	return keys
}

func (c *kMsgCarrier) Set(key, val string) {
	addStringAttribute(c.msg, key, []byte(val))
}

type msgCarrier struct {
	msg *Message
}

func (c *msgCarrier) Get(key string) string {
	for k, v := range c.msg.Headers {
		if k == key {
			return string(v)
		}
	}
	return ""
}

func (c *msgCarrier) Keys() []string {
	keys := make([]string, 0, len(c.msg.Headers))
	for k := range c.msg.Headers {
		keys = append(keys, k)
	}
	return keys
}

func (c *msgCarrier) Set(_, _ string) {}
