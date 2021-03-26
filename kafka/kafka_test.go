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
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	ewm "github.com/alexcuse/edgex-watermill/v2/core"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestKafkaConsumerConfig(t *testing.T) {
	options := ewm.WatermillConfig{
		Type:            uuid.NewString(),
		BrokerUrl:       uuid.NewString(),
		SubscribeTopics: uuid.NewString(),
		PublishTopic:    uuid.NewString(),
		WireFormat:      uuid.NewString(),
		ConsumerGroup:   uuid.NewString(),
		Optional:        nil,
	}

	watermillConfig := kafkaConsumerConfig(options)

	require.Equal(t, options.BrokerUrl, watermillConfig.Brokers[0], "should set broker URL")
	require.Equal(t, watermillConfig.ConsumerGroup, options.ConsumerGroup)

	require.Equal(t, kafka.DefaultMarshaler{}, watermillConfig.Unmarshaler)

	require.NotNil(t, watermillConfig.OverwriteSaramaConfig)
	require.Equal(t, sarama.OffsetNewest, watermillConfig.OverwriteSaramaConfig.Consumer.Offsets.Initial, "default offset")
	require.Equal(t, "watermill", watermillConfig.OverwriteSaramaConfig.ClientID)
	require.Equal(t, 256, watermillConfig.OverwriteSaramaConfig.ChannelBufferSize)
	require.True(t, watermillConfig.OverwriteSaramaConfig.Consumer.Offsets.AutoCommit.Enable)
	require.Equal(t, 1*time.Second, watermillConfig.OverwriteSaramaConfig.Consumer.Offsets.AutoCommit.Interval)
	require.Equal(t, 250*time.Millisecond, watermillConfig.OverwriteSaramaConfig.Consumer.MaxWaitTime)
	require.Equal(t, 3*time.Second, watermillConfig.OverwriteSaramaConfig.Consumer.Group.Heartbeat.Interval)
	require.Equal(t, 10*time.Second, watermillConfig.OverwriteSaramaConfig.Consumer.Group.Session.Timeout)
}
