package amqp

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	_amqp "github.com/ThreeDotsLabs/watermill-amqp/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
	ewm "github.com/alexcuse/edgex-watermill/v2/core"
	"github.com/edgexfoundry/app-functions-sdk-go/v2/pkg/interfaces"
	"github.com/edgexfoundry/go-mod-messaging/v2/messaging"
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

	switch strings.ToLower(config.WatermillFormat) {
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
	return _amqp.NewPublisher(_amqp.NewDurableQueueConfig(config.BrokerUrl), watermill.NewStdLoggerWithOut(os.Stdout, true, false))
}

func Subscriber(config ewm.WatermillConfig) (message.Subscriber, error) {
	return _amqp.NewSubscriber(_amqp.NewDurableQueueConfig(config.BrokerUrl), watermill.NewStdLoggerWithOut(os.Stdout, true, false))
}

func Trigger(tc interfaces.TriggerConfig) (interfaces.Trigger, error) {
	cfg := &ewm.WatermillConfigWrapper{}

	err := tc.ConfigLoader(cfg, "WatermillTrigger")

	if err != nil {
		return nil, err
	}

	pub, err := Publisher(cfg.WatermillTrigger)

	if err != nil {
		return nil, err
	}

	sub, err := Subscriber(cfg.WatermillTrigger)

	if err != nil {
		return nil, err
	}

	var fmt ewm.MessageFormat

	switch strings.ToLower(cfg.WatermillTrigger.WatermillFormat) {
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
		tc,
		cfg,
	), nil
}

func Register(svc interfaces.ApplicationService) {
	svc.RegisterCustomTriggerFactory("amqp-watermill", Trigger)
}
