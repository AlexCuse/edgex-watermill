package nats

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	_nats "github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	ewm "github.com/alexcuse/edgex-watermill/v2/core"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
	"github.com/nats-io/stan.go"
	"os"
	"strings"
)

func Client(ctx context.Context, config ewm.WatermillConfig) (messaging.MessageClient, error) {
	pub, err := Publisher(config)

	if err != nil {
		return nil, err
	}

	sub, err := Subscriber(config)

	if err != nil {
		return nil, err
	}

	var fmt ewm.MessageFormat

	switch strings.ToLower(config.Optional["WireFormat"]) {
	case "raw":
		fmt = &ewm.RawMessageFormat{}
	case "rawinput":
		fmt = &ewm.RawInputMessageFormat{}
	case "rawoutput":
		fmt = &ewm.RawOutputMessageFormat{}
	case "edgex":
	default:
		fmt = &ewm.EdgeXMessageFormat{}
	}

	return ewm.NewWatermillClient(
		ctx,
		pub,
		sub,
		fmt,
	)
}

func Publisher(config ewm.WatermillConfig) (message.Publisher, error) {
	return _nats.NewStreamingPublisher(_nats.StreamingPublisherConfig{
		ClusterID:   config.Optional["ClusterId"],
		ClientID:    fmt.Sprintf("pub-%s", config.ClientId),
		StanOptions: []stan.Option{stan.NatsURL(config.BrokerUrl)},
		Marshaler:   _nats.GobMarshaler{},
	}, watermill.NewStdLoggerWithOut(os.Stdout, true, false))
}

func Subscriber(config ewm.WatermillConfig) (message.Subscriber, error) {
	return _nats.NewStreamingSubscriber(_nats.StreamingSubscriberConfig{
		ClusterID:   config.Optional["ClusterId"],
		ClientID:    fmt.Sprintf("sub-%s", config.ClientId),
		StanOptions: []stan.Option{stan.NatsURL(config.BrokerUrl)},
		Unmarshaler: _nats.GobMarshaler{},
	}, watermill.NewStdLoggerWithOut(os.Stdout, true, false))
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

	var fmt ewm.MessageFormat

	switch strings.ToLower(wc.WatermillTrigger.WireFormat) {
	case "raw":
		fmt = &ewm.RawMessageFormat{}
	case "rawinput":
		fmt = &ewm.RawInputMessageFormat{}
	case "rawoutput":
		fmt = &ewm.RawOutputMessageFormat{}
	case "edgex":
		fmt = &ewm.EdgeXMessageFormat{}
	default:
		fmt = &ewm.EdgeXMessageFormat{}
	}

	return ewm.NewWatermillTrigger(
		pub,
		sub,
		fmt,
		wc,
		cfg,
	), nil
}
