package googlecloud

import (
	"cloud.google.com/go/pubsub"
	"context"
	"github.com/ThreeDotsLabs/watermill"
	gcp "github.com/ThreeDotsLabs/watermill-googlecloud/pkg/googlecloud"
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
	return gcp.NewPublisher(gcp.PublisherConfig{
		ProjectID:                 config.ClientId,
		DoNotCreateTopicIfMissing: false,
		ConnectTimeout:            0,
		PublishTimeout:            0,
		PublishSettings:           nil,
		ClientOptions:             nil,
		Marshaler:                 nil,
	}, watermill.NewStdLoggerWithOut(os.Stdout, true, false))
}

func Subscriber(config ewm.WatermillConfig) (message.Subscriber, error) {
	return gcp.NewSubscriber(gcp.SubscriberConfig{
		GenerateSubscriptionName:         nil,
		ProjectID:                        config.ClientId,
		TopicProjectID:                   "",
		DoNotCreateSubscriptionIfMissing: false,
		DoNotCreateTopicIfMissing:        false,
		ConnectTimeout:                   0,
		InitializeTimeout:                0,
		ReceiveSettings:                  pubsub.ReceiveSettings{},
		SubscriptionConfig:               pubsub.SubscriptionConfig{},
		ClientOptions:                   nil,
		Unmarshaler:                      nil,
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
