package zkafka

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// Option is a function that modify the client configurations
type Option func(*Client)

// LoggerOption applies logger to the client and to all writers/readers which are created
// after this call.
func LoggerOption(logger Logger) Option {
	return func(c *Client) {
		if logger != nil {
			c.logger = logger
		}
	}
}

// WithClientTracerProviderOption applies an otel tracer provider to the client and to all writers/readers which are created
func WithClientTracerProviderOption(tp trace.TracerProvider) Option {
	return func(c *Client) {
		if tp != nil {
			c.tp = tp
		}
	}
}

// WithClientTextMapPropagator applies an otel p to the client and to all writers/readers which are created
func WithClientTextMapPropagator(p propagation.TextMapPropagator) Option {
	return func(c *Client) {
		c.p = p
	}
}

// KafkaGroupPrefixOption creates a groupPrefix which will be added to all client and producer groupID
// strings if created after this option is added
func KafkaGroupPrefixOption(prefix string) Option {
	return func(c *Client) {
		c.groupPrefix = prefix
	}
}

func WithClientLifecycleHooks(h LifecycleHooks) Option {
	return func(c *Client) {
		c.lifecycle = h
	}
}

// WithConsumerProvider allows for the specification of a factory which is responsible for returning
// a KafkaConsumer given a config map.
func WithConsumerProvider(provider func(config map[string]any) (KafkaConsumer, error)) Option {
	return func(c *Client) {
		c.consumerProvider = func(kConfigMap kafka.ConfigMap) (KafkaConsumer, error) {
			if provider == nil {
				return nil, nil
			}
			configMap := map[string]any{}
			for k, v := range kConfigMap {
				configMap[k] = v
			}
			return provider(configMap)
		}
	}
}

// WithProducerProvider allows for the specification of a factory which is responsible for returning
// a KafkaProducer given a config map.
func WithProducerProvider(provider func(config map[string]any) (KafkaProducer, error)) Option {
	return func(c *Client) {
		c.producerProvider = func(kConfigMap kafka.ConfigMap) (KafkaProducer, error) {
			if provider == nil {
				return nil, nil
			}
			configMap := map[string]any{}
			for k, v := range kConfigMap {
				configMap[k] = v
			}
			return provider(configMap)
		}
	}
}
