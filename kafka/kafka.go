package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	ewm "github.com/alexcuse/edgex-watermill"
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

	if qc, found := options.Optional["QueueCapacity"]; found {
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

	if rt, found := options.Optional["RebalanceTimeout"]; found {
		rto, err := time.ParseDuration(rt)

		if err != nil {
			panic(err)
		}

		saramaConfig.Consumer.Group.Rebalance.Timeout = rto
	}

	if jgb, found := options.Optional["JoinGroupBackoff"]; found {
		jgbo, err := time.ParseDuration(jgb)

		if err != nil {
			panic(err)
		}

		saramaConfig.Consumer.Group.Rebalance.Retry.Backoff = jgbo
	}

	if rt, found := options.Optional["RetentionTime"]; found {
		rtime, err := time.ParseDuration(rt)

		if err != nil {
			panic(err)
		}

		saramaConfig.Consumer.Offsets.Retention = rtime
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
		Brokers:                []string{options.SubscribeHost.Host + ":" + strconv.Itoa(options.SubscribeHost.Port)},
		Unmarshaler:            kafka.DefaultMarshaler{},
		OverwriteSaramaConfig:  saramaConfig,
		ConsumerGroup:          options.Optional["ConsumerGroupID"],
		NackResendSleep:        0,
		ReconnectRetrySleep:    0,
		InitializeTopicDetails: nil,
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

	if qc, found := options.Optional["QueueCapacity"]; found {
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

		saramaConfig.Producer.Flush.Frequency = ci
	}

	if ma, found := options.Optional["MaxAttempts"]; found {
		mxa, err := strconv.Atoi(ma)

		if err != nil {
			panic(err)
		}

		saramaConfig.Producer.Retry.Max = mxa
	}

	if bs, found := options.Optional["BatchSize"]; found {
		bsz, err := strconv.Atoi(bs)

		if err != nil {
			panic(err)
		}

		saramaConfig.Producer.Flush.Messages = bsz
	}

	if bb, found := options.Optional["BatchBytes"]; found {
		bby, err := strconv.Atoi(bb)

		if err != nil {
			panic(err)
		}

		saramaConfig.Producer.Flush.Bytes = bby
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
		p, err := kafka.NewPublisher(kafkaProducerConfig(config), watermill.NewCaptureLogger())

		if err != nil {
			return nil, err
		}

		pub = p
	}

	if config.SubscribeHost.Host != "" {
		s, err := kafka.NewSubscriber(kafkaConsumerConfig(config), watermill.NewCaptureLogger())

		if err != nil {
			return nil, err
		}

		sub = s
	}
	return ewm.NewWatermillClient(
		ctx,
		pub,
		sub,
	)
}
