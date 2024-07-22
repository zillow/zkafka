package zstreams

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type confluentConsumerProvider func(configMap kafka.ConfigMap) (KafkaConsumer, error)

type defaultConfluentConsumerProvider struct {
}

func (p defaultConfluentConsumerProvider) NewConsumer(configMap kafka.ConfigMap) (KafkaConsumer, error) {
	return kafka.NewConsumer(&configMap)
}

type confluentProducerProvider func(configMap kafka.ConfigMap) (KafkaProducer, error)

type defaultConfluentProducerProvider struct {
}

func (p defaultConfluentProducerProvider) NewProducer(configMap kafka.ConfigMap) (KafkaProducer, error) {
	return kafka.NewProducer(&configMap)
}
