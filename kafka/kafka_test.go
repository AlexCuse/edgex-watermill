//
// Copyright (c) 2020 Alex Ullrich
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestKafkaConsumerConfig(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{},
		SubscribeHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		Type: "by now it better be kafka",
		Optional: map[string]string{
			"ClientID":          "client-id",
			"ChannelBufferSize": "50",
			"CommitInterval":    "1s",
			"MinBytes":          "1234",
			"MaxBytes":          "5678",
			"MaxWait":           "15s",
			"HeartbeatInterval": "20s",
			"SessionTimeout":    "60s",
		},
	}

	watermillConfig := kafkaConsumerConfig(options)

	require.Equal(t, fmt.Sprintf("%s:%d", options.SubscribeHost.Host, options.SubscribeHost.Port), watermillConfig.Brokers[0], "should set broker URL")

	require.Equal(t, watermillConfig.Unmarshaler, kafka.DefaultMarshaler{})

	require.Equal(t, watermillConfig.ConsumerGroup, options.Optional["ConsumerGroupID"])

	require.NotNil(t, watermillConfig.OverwriteSaramaConfig)
	require.Equal(t, sarama.OffsetNewest, watermillConfig.OverwriteSaramaConfig.Consumer.Offsets.Initial, "default offset")
	require.Equal(t, options.Optional["ClientID"], watermillConfig.OverwriteSaramaConfig.ClientID)
	require.Equal(t, 50, watermillConfig.OverwriteSaramaConfig.ChannelBufferSize)
	require.True(t, watermillConfig.OverwriteSaramaConfig.Consumer.Offsets.AutoCommit.Enable)
	require.Equal(t, 1*time.Second, watermillConfig.OverwriteSaramaConfig.Consumer.Offsets.AutoCommit.Interval)
	require.Equal(t, int32(1234), watermillConfig.OverwriteSaramaConfig.Consumer.Fetch.Min)
	require.Equal(t, int32(5678), watermillConfig.OverwriteSaramaConfig.Consumer.Fetch.Max)
	require.Equal(t, 15*time.Second, watermillConfig.OverwriteSaramaConfig.Consumer.MaxWaitTime)
	require.Equal(t, 20*time.Second, watermillConfig.OverwriteSaramaConfig.Consumer.Group.Heartbeat.Interval)
	require.Equal(t, 60*time.Second, watermillConfig.OverwriteSaramaConfig.Consumer.Group.Session.Timeout)
}

func TestKafkaConsumerConfig_OffsetNewest(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{},
		SubscribeHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		Type: "by now it better be kafka",
		Optional: map[string]string{
			"StartOffset": "newest",
		},
	}

	watermillConfig := kafkaConsumerConfig(options)
	require.Equal(t, sarama.OffsetNewest, watermillConfig.OverwriteSaramaConfig.Consumer.Offsets.Initial, "default offset")
}

func TestKafkaConsumerConfig_OffsetNewestNumeric(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{},
		SubscribeHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		Type: "by now it better be kafka",
		Optional: map[string]string{
			"StartOffset": "-1",
		},
	}

	watermillConfig := kafkaConsumerConfig(options)
	require.Equal(t, sarama.OffsetNewest, watermillConfig.OverwriteSaramaConfig.Consumer.Offsets.Initial, "default offset")
}

func TestKafkaConsumerConfig_OffsetOldest(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{},
		SubscribeHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		Type: "by now it better be kafka",
		Optional: map[string]string{
			"StartOffset": "oldest",
		},
	}

	watermillConfig := kafkaConsumerConfig(options)
	require.Equal(t, sarama.OffsetOldest, watermillConfig.OverwriteSaramaConfig.Consumer.Offsets.Initial, "default offset")
}

func TestKafkaConsumerConfig_OffsetOldestNumeric(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{},
		SubscribeHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		Type: "by now it better be kafka",
		Optional: map[string]string{
			"StartOffset": "-2",
		},
	}

	watermillConfig := kafkaConsumerConfig(options)
	require.Equal(t, sarama.OffsetOldest, watermillConfig.OverwriteSaramaConfig.Consumer.Offsets.Initial, "default offset")
}

func TestKafkaProducerConfig(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		SubscribeHost: types.HostInfo{},
		Type:          "by now it better be kafka",
		Optional: map[string]string{
			"ClientID":          "client-id",
			"ChannelBufferSize": "50",
			"WriteTimeout":      "3s",
			"RetryMax":          "10",
			"RetryBackoff":      "299ms",
		},
	}

	watermillConfig := kafkaProducerConfig(options)

	require.Equal(t, fmt.Sprintf("%s:%d", options.PublishHost.Host, options.PublishHost.Port), watermillConfig.Brokers[0], "should set broker URL")

	require.Equal(t, watermillConfig.Marshaler, kafka.DefaultMarshaler{})

	require.NotNil(t, watermillConfig.OverwriteSaramaConfig)
	require.Equal(t, sarama.WaitForLocal, watermillConfig.OverwriteSaramaConfig.Producer.RequiredAcks, "default offset")
	require.Equal(t, options.Optional["ClientID"], watermillConfig.OverwriteSaramaConfig.ClientID)
	require.Equal(t, 50, watermillConfig.OverwriteSaramaConfig.ChannelBufferSize)
	require.Equal(t, 3*time.Second, watermillConfig.OverwriteSaramaConfig.Producer.Timeout)
	require.Equal(t, 10, watermillConfig.OverwriteSaramaConfig.Producer.Retry.Max)
	require.Equal(t, 299*time.Millisecond, watermillConfig.OverwriteSaramaConfig.Producer.Retry.Backoff)
}

func TestKafkaProducerConfig_RequiredAcks_None(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		SubscribeHost: types.HostInfo{},
		Type:          "by now it better be kafka",
		Optional: map[string]string{
			"RequiredAcks": "none",
		},
	}

	watermillConfig := kafkaProducerConfig(options)

	require.NotNil(t, watermillConfig.OverwriteSaramaConfig)
	require.Equal(t, sarama.RequiredAcks(0), watermillConfig.OverwriteSaramaConfig.Producer.RequiredAcks, "default offset")
}

func TestKafkaProducerConfig_RequiredAcks_NoneNumeric(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		SubscribeHost: types.HostInfo{},
		Type:          "by now it better be kafka",
		Optional: map[string]string{
			"RequiredAcks": "0",
		},
	}

	watermillConfig := kafkaProducerConfig(options)

	require.NotNil(t, watermillConfig.OverwriteSaramaConfig)
	require.Equal(t, sarama.RequiredAcks(0), watermillConfig.OverwriteSaramaConfig.Producer.RequiredAcks, "default offset")
}
func TestKafkaProducerConfig_RequiredAcks_One(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		SubscribeHost: types.HostInfo{},
		Type:          "by now it better be kafka",
		Optional: map[string]string{
			"RequiredAcks": "one",
		},
	}

	watermillConfig := kafkaProducerConfig(options)

	require.NotNil(t, watermillConfig.OverwriteSaramaConfig)
	require.Equal(t, sarama.RequiredAcks(1), watermillConfig.OverwriteSaramaConfig.Producer.RequiredAcks, "default offset")
}

func TestKafkaProducerConfig_RequiredAcks_OneNumeric(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		SubscribeHost: types.HostInfo{},
		Type:          "by now it better be kafka",
		Optional: map[string]string{
			"RequiredAcks": "1",
		},
	}

	watermillConfig := kafkaProducerConfig(options)

	require.NotNil(t, watermillConfig.OverwriteSaramaConfig)
	require.Equal(t, sarama.RequiredAcks(1), watermillConfig.OverwriteSaramaConfig.Producer.RequiredAcks, "default offset")
}

func TestKafkaProducerConfig_RequiredAcks_All(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		SubscribeHost: types.HostInfo{},
		Type:          "by now it better be kafka",
		Optional: map[string]string{
			"RequiredAcks": "all",
		},
	}

	watermillConfig := kafkaProducerConfig(options)

	require.NotNil(t, watermillConfig.OverwriteSaramaConfig)
	require.Equal(t, sarama.RequiredAcks(-1), watermillConfig.OverwriteSaramaConfig.Producer.RequiredAcks, "default offset")
}

func TestKafkaProducerConfig_RequiredAcks_AllNumeric(t *testing.T) {
	options := types.MessageBusConfig{
		PublishHost: types.HostInfo{
			Host:     uuid.New().String(),
			Protocol: "tcp",
			Port:     9092,
		},
		SubscribeHost: types.HostInfo{},
		Type:          "by now it better be kafka",
		Optional: map[string]string{
			"RequiredAcks": "-1",
		},
	}

	watermillConfig := kafkaProducerConfig(options)

	require.NotNil(t, watermillConfig.OverwriteSaramaConfig)
	require.Equal(t, sarama.RequiredAcks(-1), watermillConfig.OverwriteSaramaConfig.Producer.RequiredAcks, "default offset")
}
