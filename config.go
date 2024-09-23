package zkafka

import (
	"errors"
	"fmt"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/zillow/zfmt"
)

const (
	// librdkafka configuration keys. For full definitions visit https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
	clientID                         = "client.id"
	groupID                          = "group.id"
	isolationLevel                   = "isolation.level"
	enableIdempotence                = "enable.idempotence"
	bootstrapServers                 = "bootstrap.servers"
	saslUsername                     = "sasl.username"
	saslPassword                     = "sasl.password"
	saslMechanism                    = "sasl.mechanism"
	securityProtocol                 = "security.protocol"
	requestRequiredAcks              = "request.required.acks"
	maxInFlightRequestsPerConnection = "max.in.flight.requests.per.connection"
	deliveryTimeoutMs                = "delivery.timeout.ms"
	enableAutoCommit                 = "enable.auto.commit"
	autoCommitIntervalMs             = "auto.commit.interval.ms"
	enableAutoOffsetStore            = "enable.auto.offset.store"
	maxPollIntervalMs                = "max.poll.interval.ms"
	sessionTimeoutMs                 = "session.timeout.ms"
	lingerMs                         = "linger.ms"
	socketNagleDisable               = "socket.nagle.disable"
)

// Config holds configuration to create underlying kafka client
type Config struct {
	// BootstrapServers is a list of broker addresses
	BootstrapServers []string

	// SaslUsername and SaslPassword for accessing Kafka Cluster
	SaslUsername *string
	SaslPassword *string

	// CAFile, KeyFile, CertFile are used to enable TLS with valid configuration
	// If not defined, TLS with InsecureSkipVerify=false is used.
	CAFile   string
	KeyFile  string
	CertFile string
}

// ConsumerTopicConfig holds configuration to create reader for a kafka topic
type ConsumerTopicConfig struct {
	// ClientID is required and should be unique. This is used as a cache key for the client
	ClientID string

	// GroupID is required for observability per ZG Kafka Best Practices
	// http://analytics.pages.zgtools.net/data-engineering/data-infra/streamz/docs/#/guides/kafka-guidelines?id=observability
	// The convention is [team_name]/[service]/[group], e.g. concierge/search/index-reader
	GroupID string

	// Topic is the name of the topic to be consumed. At least one should be specified between the Topic and Topics attributes
	Topic string

	// Topics are the names of the topics to be consumed. At least one should be specified between the Topic and Topics attributes
	Topics []string

	// BootstrapServers are the addresses of the possible brokers to be connected to.
	// If not defined, Reader and Writer will attempt to use the brokers defined by the client
	BootstrapServers []string

	// AutoCommitIntervalMs is a setting which indicates how often offsets will be committed to the kafka broker.
	AutoCommitIntervalMs *int

	// AdditionalProps is defined as an escape hatch to specify properties not specified as strongly typed fields.
	// The values here will be overwritten by the values of TopicConfig fields if specified there as well.
	AdditionalProps map[string]interface{}

	// Formatter is json if not defined
	Formatter zfmt.FormatterType

	SchemaRegistry SchemaRegistryConfig

	// SchemaID defines the schema registered with Confluent schema Registry
	// Default value is 0, and it implies that both Writer and Reader do not care about schema validation
	// and should encode/decode the message based on data type provided.
	// Currently, this only works with SchematizedAvroFormatter
	SchemaID int

	// Enable kafka transaction, default to false
	Transaction bool

	// ReadTimeoutMillis specifies how much time, in milliseconds, before a kafka read times out (and error is returned)
	ReadTimeoutMillis *int

	// ProcessTimeoutMillis specifies how much time, in milliseconds,
	// is given to process a particular message before cancellation is calls.
	// Default to 1 minute
	ProcessTimeoutMillis *int

	// SessionTimeoutMillis specifies how much time, in milliseconds,
	// is given by the broker, where in the absence of a heartbeat being successfully received from the consumer
	// group member, the member is considered failed (and a rebalance is initiated).
	// Defaults to 1 minute 1 second (just over default `ProcessTimeoutMillis`)
	SessionTimeoutMillis *int

	// MaxPollIntervalMillis specifies how much time, in milliseconds,
	// is given by the broker, where in the absence of `Read`/`Poll` being called by a consumer, the member is considered failed (and a rebalance is initiated).
	// Defaults to 1 minute 1 second (just over default `ProcessTimeoutMillis`)
	MaxPollIntervalMillis *int

	// ProcessDelayMillis specifies how much time, in milliseconds,
	// a virtual partition processor should pause prior to calling processor.
	// The specified duration represents the maximum pause a processor will execute. The virtual partition processor
	// uses the message's timestamp and its local estimate of `now` to determine
	// the observed delay. If the observed delay is less than the amount configured here,
	// an additional pause is executed.
	ProcessDelayMillis *int

	// SaslUsername and SaslPassword for accessing Kafka Cluster
	SaslUsername *string
	SaslPassword *string

	// DeadLetterTopicConfig allows you to specify a topic for which to write messages which failed during processing to
	DeadLetterTopicConfig *ProducerTopicConfig
}

func (p ConsumerTopicConfig) GetFormatter() zfmt.FormatterType {
	return p.Formatter
}

func (p ConsumerTopicConfig) GetSchemaID() int {
	return p.SchemaID
}

// topics returns a logical slice of the topics specified in the configuration,
// a combination of the singular Topic and enumerable Topics. It removes any empty topicNames
func (p ConsumerTopicConfig) topics() []string {
	topics := make([]string, 0, len(p.Topics)+1)
	if p.Topic != "" {
		topics = append(topics, p.Topic)
	}
	for _, t := range p.Topics {
		if t == "" {
			continue
		}
		topics = append(topics, t)
	}
	return topics
}

// ProducerTopicConfig holds configuration to create writer to kafka topic
type ProducerTopicConfig struct {
	// ClientID is required and should be unique. This is used as a cache key for the client
	ClientID string

	// Topic is required
	Topic string

	// BootstrapServers are the addresses of the possible brokers to be connected to.
	// If not defined, Reader and Writer will attempt to use the brokers defined by the client
	BootstrapServers []string

	// DeliveryTimeoutMs is a librdkafka setting. Local message timeout.
	// This value is only enforced locally and limits the time a produced message waits for successful delivery.
	// A time of 0 is infinite. This is the maximum time librdkafka may use to deliver a message (including retries).
	// Delivery error occurs when either the retry count or the message timeout are exceeded.
	// See defaults in librdkafka configuration
	DeliveryTimeoutMs *int

	// AdditionalProps is defined as an escape hatch to specify properties not specified as strongly typed fields.
	// The values here will be overwritten by the values of TopicConfig fields if specified there as well.
	AdditionalProps map[string]interface{}

	// Formatter is json if not defined
	Formatter zfmt.FormatterType

	// SchemaRegistry provides details about connecting to a schema registry including URL
	// as well as others.
	SchemaRegistry SchemaRegistryConfig

	// SchemaID defines the schema registered with Confluent schema Registry
	// Default value is 0, and it implies that both Writer and Reader do not care about schema validation
	// and should encode/decode the message based on data type provided.
	// Currently, this only works with SchematizedAvroFormatter
	SchemaID int

	// Enable kafka transaction, default to false
	Transaction bool

	// RequestRequiredAcks indicates the number of acknowledgments the leader broker must receieve from In Sync Replica (ISR) brokers before responding
	// to the request (0=Broker does not send any response to client, -1 or all=broker blocks until all ISRs commit)
	RequestRequiredAcks *string

	// EnableIdempotence When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order.
	// Default to true
	EnableIdempotence *bool

	// LingerMillis specifies the delay, in milliseconds, to wait for messages in the producer to accumulate before constructing message batches.
	LingerMillis int

	NagleDisable *bool

	// SaslUsername and SaslPassword for accessing Kafka Cluster
	SaslUsername *string
	SaslPassword *string
}

func (p ProducerTopicConfig) GetFormatter() zfmt.FormatterType {
	return p.Formatter
}

func (p ProducerTopicConfig) GetSchemaID() int {
	return p.SchemaID
}

type SchemaRegistryConfig struct {
	// URL is the schema registry URL. During serialization and deserialization
	// schema registry is checked against to confirm schema compatability.
	URL string
	// Serialization provides additional information used by schema registry formatters during serialization (data write)
	Serialization SerializationConfig
	// Deserialization provides additional information used by schema registry formatters during deserialization (data read)
	Deserialization DeserializationConfig
}

type SerializationConfig struct {
	// AutoRegisterSchemas indicates whether new schemas (those that evolve existing schemas or are brand new) should be registered
	// with schema registry dynamically. This feature is typically not used for production workloads
	AutoRegisterSchemas bool
	// Schema is used exclusively by the avro schema registry formatter today. Its necessary to provide proper schema evolution properties
	// expected by typical use cases.
	Schema string
}

type DeserializationConfig struct {
}

func getDefaultConsumerTopicConfig(topicConfig *ConsumerTopicConfig) error {
	if topicConfig.ClientID == "" {
		return errors.New("invalid config, ClientID must not be empty")
	}
	if topicConfig.GroupID == "" {
		return errors.New("invalid config, group name cannot be empty")
	}
	if len(topicConfig.topics()) == 0 {
		return errors.New("invalid config, missing topic name")
	}

	if string(topicConfig.Formatter) == "" {
		// default to json formatter
		topicConfig.Formatter = zfmt.JSONFmt
	}

	const defaultProcessTimeoutMillis = 60 * 1000
	if topicConfig.ProcessTimeoutMillis == nil || *topicConfig.ProcessTimeoutMillis == 0 {
		topicConfig.ProcessTimeoutMillis = ptr(defaultProcessTimeoutMillis)
	}
	const defaultSessionTimeoutMillis = 61 * 1000
	if topicConfig.SessionTimeoutMillis == nil || *topicConfig.SessionTimeoutMillis <= 0 {
		topicConfig.SessionTimeoutMillis = ptr(defaultSessionTimeoutMillis)
	}
	const defaultMaxPollTimeoutMillis = 61 * 1000
	if topicConfig.MaxPollIntervalMillis == nil || *topicConfig.MaxPollIntervalMillis <= 0 {
		topicConfig.MaxPollIntervalMillis = ptr(defaultMaxPollTimeoutMillis)
	}

	var defaultReadTimeoutMillis = 1000
	if topicConfig.ReadTimeoutMillis == nil || *topicConfig.ReadTimeoutMillis <= 0 {
		topicConfig.ReadTimeoutMillis = &defaultReadTimeoutMillis
	}

	return nil
}

func getDefaultProducerTopicConfig(topicConfig *ProducerTopicConfig) error {
	if topicConfig.ClientID == "" {
		return errors.New("invalid config, ClientID must not be empty")
	}
	if topicConfig.Topic == "" {
		return errors.New("invalid config, missing topic name")
	}
	if topicConfig.NagleDisable == nil {
		topicConfig.NagleDisable = ptr(true)
	}

	if string(topicConfig.Formatter) == "" {
		// default to json formatter
		topicConfig.Formatter = zfmt.JSONFmt
	}

	return nil
}

// makeConsumerConfig creates a kafka configMap from the specified strongly typed Config and TopicConfig.
// TopicConfig specifies a way to specify config values that aren't strongly typed via AdditionalProps field.
// Those values are overwritten if specified in strongly typed TopicConfig fields.
func makeConsumerConfig(conf Config, topicConfig ConsumerTopicConfig, prefix string) (kafka.ConfigMap, error) {
	configMap := kafka.ConfigMap{}

	configMap[clientID] = topicConfig.ClientID
	grp := topicConfig.GroupID
	if prefix != "" {
		grp = fmt.Sprintf("%s.%s", prefix, grp)
	}
	configMap[groupID] = grp
	configMap[enableAutoCommit] = true

	if topicConfig.MaxPollIntervalMillis != nil {
		configMap[maxPollIntervalMs] = *topicConfig.MaxPollIntervalMillis
	}

	if topicConfig.SessionTimeoutMillis != nil {
		configMap[sessionTimeoutMs] = *topicConfig.SessionTimeoutMillis
	}

	// per https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#auto-offset-commit we can control which offsets
	// are eligible for commit by storing this to false and calling rd_kafka_offsets_store ourselves (via StoreOffsets)
	configMap[enableAutoOffsetStore] = false

	if topicConfig.AutoCommitIntervalMs != nil {
		configMap[autoCommitIntervalMs] = *topicConfig.AutoCommitIntervalMs
	}

	// specific settings to enable transaction API (this is actually a poor man's transaction, you still need some library help
	// which isn't currently implemented in lib)
	if topicConfig.Transaction {
		configMap[isolationLevel] = "read_committed"
	}

	// overwrite BootstrapServers if defined per topic config
	addresses := conf.BootstrapServers
	if len(topicConfig.BootstrapServers) != 0 {
		addresses = topicConfig.BootstrapServers
	}
	if len(addresses) == 0 {
		return nil, errors.New("invalid consumer config, missing bootstrap server addresses")
	}
	configMap[bootstrapServers] = strings.Join(addresses, ",")

	saslUname := conf.SaslUsername
	if topicConfig.SaslUsername != nil {
		saslUname = topicConfig.SaslUsername
	}

	saslPwd := conf.SaslPassword
	if topicConfig.SaslPassword != nil {
		saslPwd = topicConfig.SaslPassword
	}

	if saslUname != nil && saslPwd != nil && len(*saslUname) > 0 && len(*saslPwd) > 0 {
		configMap[securityProtocol] = "SASL_SSL"
		configMap[saslMechanism] = "SCRAM-SHA-256"
		configMap[saslUsername] = *saslUname
		configMap[saslPassword] = *saslPwd
	}

	for key, val := range topicConfig.AdditionalProps {
		// confluent-kafka-go does some limited type checking and doesn't allow for floats64.
		// We'll convert these to int and store them in the map
		switch v := val.(type) {
		case float64:
			configMap[key] = kafka.ConfigValue(int(v))
		case float32:
			configMap[key] = kafka.ConfigValue(int(v))
		default:
			configMap[key] = kafka.ConfigValue(v)
		}
	}
	return configMap, nil
}

// makeProducerConfig creates a kafka configMap from the specified strongly typed Config and TopicConfig.
// TopicConfig specifies a way to specify config values that aren't strongly typed via AdditionalProps field.
// Those values are overwritten if specified in strongly typed TopicConfig fields.
func makeProducerConfig(conf Config, topicConfig ProducerTopicConfig) (kafka.ConfigMap, error) {
	configMap := kafka.ConfigMap{}

	configMap[clientID] = getWriterKey(topicConfig)

	if topicConfig.RequestRequiredAcks != nil {
		configMap[requestRequiredAcks] = *topicConfig.RequestRequiredAcks
	}
	configMap[enableIdempotence] = true
	if topicConfig.EnableIdempotence != nil {
		configMap[enableIdempotence] = *topicConfig.EnableIdempotence
	}

	if topicConfig.DeliveryTimeoutMs != nil {
		configMap[deliveryTimeoutMs] = *topicConfig.DeliveryTimeoutMs
	}

	if topicConfig.LingerMillis >= 0 {
		configMap[lingerMs] = topicConfig.LingerMillis
	}

	if topicConfig.NagleDisable != nil {
		configMap[socketNagleDisable] = *topicConfig.NagleDisable
	}

	// specific settings to enable transaction API (this is actually a poor man's transaction, you still need some library help
	// which isn't currently implemented in lib)
	if topicConfig.Transaction {
		configMap[enableIdempotence] = true

		configMap[requestRequiredAcks] = -1

		configMap[maxInFlightRequestsPerConnection] = 1
	}

	// overwrite BootstrapServers if defined per topic config
	addresses := conf.BootstrapServers
	if len(topicConfig.BootstrapServers) != 0 {
		addresses = topicConfig.BootstrapServers
	}
	if len(addresses) == 0 {
		return nil, errors.New("invalid producer config, missing bootstrap server addresses")
	}

	configMap[bootstrapServers] = strings.Join(addresses, ",")

	saslUname := conf.SaslUsername
	if topicConfig.SaslUsername != nil {
		saslUname = topicConfig.SaslUsername
	}

	saslPwd := conf.SaslPassword
	if topicConfig.SaslPassword != nil {
		saslPwd = topicConfig.SaslPassword
	}

	if saslUname != nil && saslPwd != nil && len(*saslUname) > 0 && len(*saslPwd) > 0 {
		configMap[securityProtocol] = "SASL_SSL"
		configMap[saslMechanism] = "SCRAM-SHA-256"
		configMap[saslUsername] = *saslUname
		configMap[saslPassword] = *saslPwd
	}

	for key, val := range topicConfig.AdditionalProps {
		// confluent-kafka-go does some limited type checking and doesn't allow for floats64.
		// We'll convert these to int and store them in the map
		switch v := val.(type) {
		case float64:
			configMap[key] = kafka.ConfigValue(int(v))
		case float32:
			configMap[key] = kafka.ConfigValue(int(v))
		default:
			configMap[key] = kafka.ConfigValue(v)
		}
	}
	return configMap, nil
}
