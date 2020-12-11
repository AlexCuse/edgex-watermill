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
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	ewm "github.com/alexcuse/edgex-watermill"
	"github.com/edgexfoundry/app-functions-sdk-go/appsdk"
	"github.com/edgexfoundry/go-mod-messaging/messaging"
	"github.com/edgexfoundry/go-mod-messaging/pkg/types"
	"strconv"
	"strings"
	"time"
)

func kafkaConsumerConfig(options types.MessageBusConfig) kafka.SubscriberConfig {
	saramaConfig := kafka.DefaultSaramaSubscriberConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest

	if kv, found := options.Optional["KafkaVersion"]; found {
		var err error

		saramaConfig.Version, err = sarama.ParseKafkaVersion(kv)

		if err != nil {
			panic(err)
		}
	}

	if clientid, found := options.Optional["ClientID"]; found {
		saramaConfig.ClientID = clientid
	}

	if qc, found := options.Optional["ChannelBufferSize"]; found {
		qcap, err := strconv.Atoi(qc)

		if err != nil {
			panic(err)
		}

		saramaConfig.ChannelBufferSize = qcap
	}

	if cmi, found := options.Optional["CommitInterval"]; found {
		ci, err := time.ParseDuration(cmi)

		if err != nil {
			panic(err)
		}

		saramaConfig.Consumer.Offsets.AutoCommit.Interval = ci
		saramaConfig.Consumer.Offsets.AutoCommit.Enable = true
	}

	if mb, found := options.Optional["MinBytes"]; found {
		mbytes, err := strconv.ParseInt(mb, 10, 32)

		if err != nil {
			panic(err)
		}

		saramaConfig.Consumer.Fetch.Min = int32(mbytes)
	}

	if mb, found := options.Optional["MaxBytes"]; found {
		mbytes, err := strconv.ParseInt(mb, 10, 32)

		if err != nil {
			panic(err)
		}

		saramaConfig.Consumer.Fetch.Max = int32(mbytes)
	}

	if mw, found := options.Optional["MaxWait"]; found {
		mwait, err := time.ParseDuration(mw)

		if err != nil {
			panic(err)
		}

		saramaConfig.Consumer.MaxWaitTime = mwait
	}

	if hb, found := options.Optional["HeartbeatInterval"]; found {
		hbi, err := time.ParseDuration(hb)

		if err != nil {
			panic(err)
		}

		saramaConfig.Consumer.Group.Heartbeat.Interval = hbi
	}

	if st, found := options.Optional["SessionTimeout"]; found {
		sto, err := time.ParseDuration(st)

		if err != nil {
			panic(err)
		}

		saramaConfig.Consumer.Group.Session.Timeout = sto
	}

	if so, found := options.Optional["StartOffset"]; found {
		if so == "-1" || strings.EqualFold(so, "newest") {
			saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
		} else if so == "-2" || strings.EqualFold(so, "oldest") {
			saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
		} else {
			panic("StartOffset must be -1/newest or -2/oldest")
		}
	}

	return kafka.SubscriberConfig{
		Brokers:               []string{options.SubscribeHost.Host + ":" + strconv.Itoa(options.SubscribeHost.Port)},
		Unmarshaler:           kafka.DefaultMarshaler{},
		OverwriteSaramaConfig: saramaConfig,
		ConsumerGroup:         options.Optional["ConsumerGroupID"],
	}
}

func kafkaProducerConfig(options types.MessageBusConfig) kafka.PublisherConfig {
	saramaConfig := kafka.DefaultSaramaSyncPublisherConfig()

	if kv, found := options.Optional["KafkaVersion"]; found {
		var err error

		saramaConfig.Version, err = sarama.ParseKafkaVersion(kv)

		if err != nil {
			panic(err)
		}
	}

	if clientid, found := options.Optional["ClientID"]; found {
		saramaConfig.ClientID = clientid
	}

	if qc, found := options.Optional["ChannelBufferSize"]; found {
		qcap, err := strconv.Atoi(qc)

		if err != nil {
			panic(err)
		}

		saramaConfig.ChannelBufferSize = qcap
	}

	if wt, found := options.Optional["WriteTimeout"]; found {
		wto, err := time.ParseDuration(wt)

		if err != nil {
			panic(err)
		}

		saramaConfig.Producer.Timeout = wto
	}

	if req, found := options.Optional["RequiredAcks"]; found {
		switch strings.ToLower(req) {
		case "all", "-1":
			saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(-1)
			break
		case "one", "1":
			saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(1)
			break
		case "none", "0":
			saramaConfig.Producer.RequiredAcks = sarama.RequiredAcks(0)
			break
		}
	}

	if rm, found := options.Optional["RetryMax"]; found {
		rmi, err := strconv.Atoi(rm)

		if err != nil {
			panic(err)
		}

		saramaConfig.Producer.Retry.Max = rmi
	}

	if rb, found := options.Optional["RetryBackoff"]; found {
		rbd, err := time.ParseDuration(rb)

		if err != nil {
			panic(err)
		}

		saramaConfig.Producer.Retry.Backoff = rbd
	}

	return kafka.PublisherConfig{
		Brokers:               []string{options.PublishHost.Host + ":" + strconv.Itoa(options.PublishHost.Port)},
		Marshaler:             kafka.DefaultMarshaler{},
		OverwriteSaramaConfig: saramaConfig,
	}
}

func NewKafkaClient(ctx context.Context, config types.MessageBusConfig) (messaging.MessageClient, error) {
	var pub message.Publisher
	var sub message.Subscriber

	if config.PublishHost.Host != "" {
		p, err := createPublisher(config)

		if err != nil {
			return nil, err
		}

		pub = p
	}

	if config.SubscribeHost.Host != "" {
		s, err := createSubscriber(config)

		if err != nil {
			return nil, err
		}

		sub = s
	}

	var fmt ewm.MessageFormat

	switch strings.ToLower(config.Optional["WatermillFormat"]) {
	case "raw":
		fmt = &ewm.RawMessageFormat{}
	case "edgexjson":
	default:
		fmt = &ewm.EdgeXJSONMessageFormat{}
	}

	return ewm.NewWatermillClient(
		ctx,
		pub,
		sub,
		fmt,
	)
}

func createPublisher(config types.MessageBusConfig) (message.Publisher, error) {
	return kafka.NewPublisher(kafkaProducerConfig(config), watermill.NewCaptureLogger())
}

func createSubscriber(config types.MessageBusConfig) (message.Subscriber, error) {
	return kafka.NewSubscriber(kafkaConsumerConfig(config), watermill.NewCaptureLogger())
}

func NewKafkaTrigger(ctx context.Context, seed appsdk.ContextSeed, processor appsdk.ProcessMessageFunc) (appsdk.Trigger, error) {
	var pub message.Publisher
	var sub message.Subscriber

	if seed.Configuration.MessageBus.PublishHost.Host != "" {
		p, err := createPublisher(seed.Configuration.MessageBus)

		if err != nil {
			return nil, err
		}

		pub = p
	}

	if seed.Configuration.MessageBus.SubscribeHost.Host != "" {
		s, err := createSubscriber(seed.Configuration.MessageBus)

		if err != nil {
			return nil, err
		}

		sub = s
	}

	return ewm.NewWatermillTrigger(
		pub,
		sub,
		ctx,
		seed,
		processor,
	), nil
}
