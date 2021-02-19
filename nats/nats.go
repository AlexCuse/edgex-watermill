package nats

import (
	"context"
	"fmt"
	"github.com/ThreeDotsLabs/watermill"
	_nats "github.com/ThreeDotsLabs/watermill-nats/pkg/nats"
	"github.com/ThreeDotsLabs/watermill/message"
	ewm "github.com/alexcuse/edgex-watermill/core"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/appsdk"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
	"github.com/nats-io/stan.go"
	"os"
	"strings"
)

func Client(ctx context.Context, config types.MessageBusConfig) (messaging.MessageClient, error) {
	var pub message.Publisher
	var sub message.Subscriber

	if config.PublishHost.Host != "" {
		p, err := Publisher(config)

		if err != nil {
			return nil, err
		}

		pub = p
	}

	if config.SubscribeHost.Host != "" {
		s, err := Subscriber(config)

		if err != nil {
			return nil, err
		}

		sub = s
	}

	var fmt ewm.MessageFormat

	switch strings.ToLower(config.Optional["WatermillFormat"]) {
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

func Publisher(config types.MessageBusConfig) (message.Publisher, error) {
	return _nats.NewStreamingPublisher(_nats.StreamingPublisherConfig{
		ClusterID:   "test-cluster",
		ClientID:    fmt.Sprintf("pub-%s", config.Optional["ClientID"]),
		StanOptions: []stan.Option{stan.NatsURL(config.Optional["BrokerURL"])},
		Marshaler:   _nats.GobMarshaler{},
	}, watermill.NewStdLoggerWithOut(os.Stdout, true, false))
}

func Subscriber(config types.MessageBusConfig) (message.Subscriber, error) {
	return _nats.NewStreamingSubscriber(_nats.StreamingSubscriberConfig{
		ClusterID:   "test-cluster",
		ClientID:    fmt.Sprintf("sub-%s", config.Optional["ClientID"]),
		StanOptions: []stan.Option{stan.NatsURL(config.Optional["BrokerURL"])},
		Unmarshaler: _nats.GobMarshaler{},
	}, watermill.NewStdLoggerWithOut(os.Stdout, true, false))
}

func Trigger(tc appsdk.TriggerConfig) (appsdk.Trigger, error) {
	var pub message.Publisher
	var sub message.Subscriber

	if tc.Config.MessageBus.PublishHost.Host != "" {
		p, err := Publisher(tc.Config.MessageBus)

		if err != nil {
			return nil, err
		}

		pub = p
	}

	if tc.Config.MessageBus.SubscribeHost.Host != "" {
		s, err := Subscriber(tc.Config.MessageBus)

		if err != nil {
			return nil, err
		}

		sub = s
	}

	var fmt ewm.MessageFormat

	switch strings.ToLower(tc.Config.MessageBus.Optional["WatermillFormat"]) {
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

	return ewm.NewWatermillTrigger(
		pub,
		sub,
		fmt,
		tc,
	), nil
}

func Register(sdk *appsdk.AppFunctionsSDK) {
	sdk.RegisterCustomTriggerFactory("nats-watermill", Trigger)
}
