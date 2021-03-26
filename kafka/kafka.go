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
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	ewm "github.com/alexcuse/edgex-watermill/v2/core"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
	"strings"
)

func kafkaConsumerConfig(config ewm.WatermillConfig) kafka.SubscriberConfig {
	/*
		saramaConfig := config.ConfigOverride

		if saramaConfig == nil {
			saramaConfig = kafka.DefaultSaramaSubscriberConfig()
		}
	*/

	return kafka.SubscriberConfig{
		Brokers:               []string{config.BrokerUrl},
		Unmarshaler:           kafka.DefaultMarshaler{},
		OverwriteSaramaConfig: kafka.DefaultSaramaSubscriberConfig(),
		ConsumerGroup:         config.ConsumerGroup,
	}
}

func kafkaProducerConfig(config ewm.WatermillConfig) kafka.PublisherConfig {
	return kafka.PublisherConfig{
		Brokers:               []string{config.BrokerUrl},
		Marshaler:             kafka.DefaultMarshaler{},
		OverwriteSaramaConfig: kafka.DefaultSaramaSyncPublisherConfig(),
	}
}

func Client(ctx context.Context, config ewm.WatermillConfig) (messaging.MessageClient, error) {
	var pub message.Publisher
	var sub message.Subscriber

	if config.PublishTopic != "" {
		p, err := Publisher(config)

		if err != nil {
			return nil, err
		}

		pub = p
	}

	if config.SubscribeTopics != "" {
		s, err := Subscriber(config)

		if err != nil {
			return nil, err
		}

		sub = s
	}

	var fmt ewm.WireFormat

	switch strings.ToLower(config.WireFormat) {
	case "raw":
		fmt = &ewm.RawWireFormat{}
	case "rawinput":
		fmt = &ewm.RawInputWireFormat{}
	case "rawoutput":
		fmt = &ewm.RawOutputWireFormat{}
	case "edgex":
	default:
		fmt = &ewm.EdgeXWireFormat{}
	}

	return ewm.NewWatermillClient(
		ctx,
		pub,
		sub,
		fmt,
	)
}

func Publisher(config ewm.WatermillConfig) (message.Publisher, error) {
	return kafka.NewPublisher(kafkaProducerConfig(config), watermill.NewCaptureLogger())
}

func Subscriber(config ewm.WatermillConfig) (message.Subscriber, error) {
	return kafka.NewSubscriber(kafkaConsumerConfig(config), watermill.NewCaptureLogger())
}

func Trigger(wc *ewm.WatermillConfigWrapper, cfg interfaces.TriggerConfig) (interfaces.Trigger, error) {
	pub, err := Publisher(wc.WatermillTrigger)

	if err != nil {
		return nil, err
	}

	sub, err := Subscriber(wc.WatermillTrigger)

	if err != nil {
		return nil, err
	}

	var fmt ewm.WireFormat

	switch strings.ToLower(wc.WatermillTrigger.WireFormat) {
	case "raw":
		fmt = &ewm.RawWireFormat{}
	case "rawinput":
		fmt = &ewm.RawInputWireFormat{}
	case "rawoutput":
		fmt = &ewm.RawOutputWireFormat{}
	case "edgex":
		fmt = &ewm.EdgeXWireFormat{}
	default:
		fmt = &ewm.EdgeXWireFormat{}
	}

	return ewm.NewWatermillTrigger(
		pub,
		sub,
		fmt,
		wc,
		cfg,
	), nil
}
