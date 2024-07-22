package zstreams

import (
	"testing"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"

	"gitlab.zgtools.net/devex/archetypes/gomods/zfmt"
)

func Test_getDefaultConsumerTopicConfig(t *testing.T) {
	type args struct {
		conf *ConsumerTopicConfig
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		expectedTopics []string
	}{
		{
			name: "missing required field (GroupID) => error",
			args: args{conf: &ConsumerTopicConfig{
				Topic:    "test_topic",
				ClientID: "test",
			}},
			wantErr: true,
		},
		{
			name: "missing required field (Topic) => error",
			args: args{conf: &ConsumerTopicConfig{
				GroupID:  "test_group",
				ClientID: "test",
			}},
			wantErr: true,
		},
		{
			name: "missing required non empty fields (Topic and or Topics) => error",
			args: args{conf: &ConsumerTopicConfig{
				GroupID:  "test_group",
				ClientID: "test",
				Topics:   []string{"", "", ""},
			}},
			wantErr: true,
		},
		{
			name: "missing required field (ClientID) => error",
			args: args{conf: &ConsumerTopicConfig{
				GroupID: "test_group",
				Topic:   "test_topic",
			}},
			wantErr: true,
		},
		{
			name: "has minimum required fields => no error",
			args: args{conf: &ConsumerTopicConfig{
				ClientID: "test",
				GroupID:  "test_group",
				Topic:    "test_topic",
			}},
			wantErr:        false,
			expectedTopics: []string{"test_topic"},
		},
		{
			name: "has minimum required fields (with multitopic subscription) => no error",
			args: args{conf: &ConsumerTopicConfig{
				ClientID: "test",
				GroupID:  "test_group",
				Topic:    "hello",
				Topics:   []string{"test_topic", "tt"},
			}},
			wantErr:        false,
			expectedTopics: []string{"hello", "test_topic", "tt"},
		},
		{
			name: "has minimum required fields (with a 0 process timeout) => no error",
			args: args{conf: &ConsumerTopicConfig{
				ClientID:          "test",
				GroupID:           "test_group",
				Topic:             "test_topic",
				ReadTimeoutMillis: ptr(0),
			}},
			wantErr:        false,
			expectedTopics: []string{"test_topic"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			err := getDefaultConsumerTopicConfig(tt.args.conf)
			if tt.wantErr {
				require.Error(t, err)
				return
			} else {
				require.NoError(t, err)
			}
			var defaultReadTimeoutMillis = 1000
			var defaultProcessTimeoutMillis = 60000
			var defaultSessionTimeoutMillis = 61000
			var defaultMaxPollIntervalMillis = 61000
			expectedConfig := &ConsumerTopicConfig{
				ClientID:              tt.args.conf.ClientID,
				GroupID:               tt.args.conf.GroupID,
				Formatter:             zfmt.JSONFmt,
				ReadTimeoutMillis:     &defaultReadTimeoutMillis,
				ProcessTimeoutMillis:  &defaultProcessTimeoutMillis,
				SessionTimeoutMillis:  &defaultSessionTimeoutMillis,
				MaxPollIntervalMillis: &defaultMaxPollIntervalMillis,
			}
			ignoreOpts := cmpopts.IgnoreFields(ConsumerTopicConfig{}, "Topic", "Topics")
			assertEqual(t, tt.args.conf, expectedConfig, ignoreOpts)
			assertEqual(t, tt.args.conf.topics(), tt.expectedTopics)
		})
	}
}

func Test_getDefaultProducerTopicConfig(t *testing.T) {
	type args struct {
		conf *ProducerTopicConfig
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "missing required field (Topic) => error",
			args: args{conf: &ProducerTopicConfig{
				ClientID: "test",
			}},
			wantErr: true,
		},
		{
			name: "has minimum required fields => no error",
			args: args{conf: &ProducerTopicConfig{
				ClientID: "test",
				Topic:    "test_topic",
			},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			err := getDefaultProducerTopicConfig(tt.args.conf)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			if tt.wantErr {
				return
			}
			expectedConfig := &ProducerTopicConfig{
				ClientID:     tt.args.conf.ClientID,
				Topic:        tt.args.conf.Topic,
				Formatter:    zfmt.JSONFmt,
				NagleDisable: ptr(true),
			}
			assertEqual(t, tt.args.conf, expectedConfig)
		})
	}
}

func Test_getDefaultConsumerTopicConfigSpecialCase(t *testing.T) {
	type args struct {
		conf *ConsumerTopicConfig
	}
	tests := []struct {
		name string
		args args
		want *ConsumerTopicConfig
	}{
		{
			name: "has minimum required fields with negative CacheTime => no error (adjusted to default)",
			args: args{conf: &ConsumerTopicConfig{
				ClientID:             "test",
				GroupID:              "test_group",
				Topic:                "test_topic",
				ProcessTimeoutMillis: ptr(60000),
			}},
			want: &ConsumerTopicConfig{
				ClientID:              "test",
				GroupID:               "test_group",
				Topic:                 "test_topic",
				Formatter:             zfmt.JSONFmt,
				ReadTimeoutMillis:     ptr(1000),
				ProcessTimeoutMillis:  ptr(60000),
				SessionTimeoutMillis:  ptr(61000),
				MaxPollIntervalMillis: ptr(61000),
			},
		},
		{
			name: "has minimum required fields with negative buffer => no error",
			args: args{conf: &ConsumerTopicConfig{
				ClientID: "test",
				GroupID:  "test_group",
				Topic:    "test_topic",
			}},
			want: &ConsumerTopicConfig{
				ClientID:              "test",
				GroupID:               "test_group",
				Topic:                 "test_topic",
				Formatter:             zfmt.JSONFmt,
				ReadTimeoutMillis:     ptr(1000),
				ProcessTimeoutMillis:  ptr(60000),
				SessionTimeoutMillis:  ptr(61000),
				MaxPollIntervalMillis: ptr(61000),
			},
		},
		{
			name: "has minimum required fields with positive ProcessTimeoutMillis",
			args: args{conf: &ConsumerTopicConfig{
				ClientID:              "test",
				GroupID:               "test_group",
				Topic:                 "test_topic",
				ReadTimeoutMillis:     ptr(100),
				AutoCommitIntervalMs:  ptr(12000),
				ProcessTimeoutMillis:  ptr(60000),
				SessionTimeoutMillis:  ptr(61000),
				MaxPollIntervalMillis: ptr(61000),
			}},
			want: &ConsumerTopicConfig{
				ClientID:              "test",
				GroupID:               "test_group",
				Topic:                 "test_topic",
				Formatter:             zfmt.JSONFmt,
				ReadTimeoutMillis:     ptr(100),
				AutoCommitIntervalMs:  ptr(12000),
				ProcessTimeoutMillis:  ptr(60000),
				SessionTimeoutMillis:  ptr(61000),
				MaxPollIntervalMillis: ptr(61000),
			},
		},
		{
			name: "has minimum required fields with positive AutoCommitIntervalMs",
			args: args{conf: &ConsumerTopicConfig{
				ClientID:             "test",
				GroupID:              "test_group",
				Topic:                "test_topic",
				AutoCommitIntervalMs: ptr(123),
				ProcessTimeoutMillis: ptr(60000),
			}},
			want: &ConsumerTopicConfig{
				ClientID:              "test",
				GroupID:               "test_group",
				Topic:                 "test_topic",
				Formatter:             zfmt.JSONFmt,
				ReadTimeoutMillis:     ptr(1000),
				AutoCommitIntervalMs:  ptr(123),
				ProcessTimeoutMillis:  ptr(60000),
				SessionTimeoutMillis:  ptr(61000),
				MaxPollIntervalMillis: ptr(61000),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			err := getDefaultConsumerTopicConfig(tt.args.conf)
			require.NoError(t, err)
			assertEqual(t, tt.args.conf, tt.want)
		})
	}
}

func Test_getDefaultProducerTopicConfigSpecialCase(t *testing.T) {
	type args struct {
		conf *ProducerTopicConfig
	}
	var tests []struct {
		name string
		args args
		want *ProducerTopicConfig
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer recoverThenFail(t)
			err := getDefaultProducerTopicConfig(tt.args.conf)
			require.NoError(t, err)
			assertEqual(t, tt.args.conf, tt.want)
		})
	}
}
