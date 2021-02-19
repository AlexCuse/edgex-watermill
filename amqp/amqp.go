package amqp

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	_amqp "github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	ewm "github.com/alexcuse/edgex-watermill/core"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/appsdk"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
	"github.com/edgexfoundry/go-mod-messaging/v2/pkg/types"
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
	return _amqp.NewPublisher(_amqp.NewDurableQueueConfig(config.Optional["BrokerURL"]), watermill.NewStdLoggerWithOut(os.Stdout, true, false))
}

func Subscriber(config types.MessageBusConfig) (message.Subscriber, error) {
	return _amqp.NewSubscriber(_amqp.NewDurableQueueConfig(config.Optional["BrokerURL"]), watermill.NewStdLoggerWithOut(os.Stdout, true, false))
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
	sdk.RegisterCustomTriggerFactory("amqp-watermill", Trigger)
}
